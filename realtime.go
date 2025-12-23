package main

import (
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"time"

	"github.com/MobilityData/gtfs-realtime-bindings/golang/gtfs"
	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
	"google.golang.org/protobuf/proto"
)

type ColumnInfo struct {
	Name string
	Type string
}

var columns = []ColumnInfo{
	{Name: "trip_id", Type: "TEXT"},
	{Name: "route_id", Type: "TEXT"},
	{Name: "direction_id", Type: "INT8"},
	{Name: "start_time", Type: "DATETIME"},
	{Name: "schedule_relationship", Type: "INT8"},
	{Name: "latitude", Type: "REAL"},
	{Name: "longitude", Type: "REAL"}, {Name: "bearing", Type: "REAL"},
	{Name: "odometer", Type: "REAL"},
	{Name: "speed", Type: "REAL"},
	{Name: "current_stop_sequence", Type: "INTEGER"},
	{Name: "stop_id", Type: "TEXT"},
	{Name: "current_status", Type: "INT8"},
	{Name: "timestamp", Type: "DATETIME"},
	{Name: "congestion_level", Type: "INT8"},
	{Name: "occupancy_status", Type: "INT8"},
	{Name: "vehicle_id", Type: "TEXT"},
	{Name: "vehicle_label", Type: "TEXT"},
	{Name: "license_plate", Type: "TEXT"},
}

func insertQuery() string {
	var query strings.Builder
	query.WriteString("INSERT INTO vehicle_positions (")
	for i, colInfo := range columns {
		if i > 0 {
			query.WriteByte(',')
		}
		query.WriteString(colInfo.Name)
	}
	query.WriteString(") VALUES (")
	for i, colInfo := range columns {
		if i > 0 {
			query.WriteByte(',')
		}
		query.WriteByte(':')
		query.WriteString(colInfo.Name)
	}
	query.WriteString(") ON CONFLICT DO NOTHING")

	return query.String()
}

type VehiclePosition struct {
	TripId      string `db:"trip_id" parquet:"trip_id"`
	RouteId     string `db:"route_id" parquet:"route_id,dict"`
	DirectionId int32  `db:"direction_id" parquet:"direction_id"`
	// Need to have two different fields for (de)serizialization from ProtoBuf -> SQLite -> Parquet.
	// The Go SQLite driver force converts time.Time to TEXT, so we must use an int column instead.
	// Parquet, however, does treat it as a 8-byte timestamp.
	StartTime            time.Time `db:"-" parquet:"start_time,delta"`
	StartTimeUnix        int64     `db:"start_time" parquet:"-"`
	ScheduleRelationship int32     `db:"schedule_relationship" parquet:"schedule_relationship"`
	Latitude             float32   `db:"latitude" parquet:"latitude,split"`
	Longitude            float32   `db:"longitude" parquet:"longitude,split"`
	Bearing              float32   `db:"bearing" parquet:"bearing,split"`
	Odometer             float64   `db:"odometer" parquet:"odometer,split"`
	Speed                float32   `db:"speed" parquet:"speed"`
	CurrentStopSequence  uint32    `db:"current_stop_sequence" parquet:"current_stop_sequence"`
	StopId               string    `db:"stop_id" parquet:"stop_id,dict"`
	CurrentStatus        int32     `db:"current_status" parquet:"current_status"`
	// Same treatment applies here
	Timestamp       time.Time `db:"-" parquet:"timestamp,delta"`
	TimestampUnix   int64     `db:"timestamp" parquet:"-"`
	CongestionLevel int32     `db:"congestion_level" parquet:"congestion_level"`
	OccupancyStatus int32     `db:"occupancy_status" parquet:"occupancy_status"`
	VehicleId       string    `db:"vehicle_id" parquet:"vehicle_id,dict"`
	VehicleLabel    string    `db:"vehicle_label" parquet:"vehicle_label,dict"`
	LicensePlate    string    `db:"license_plate" parquet:"license_plate,dict"`
	// Only used for partitioning in Parquet
	Year  int `parquet:"year"`
	Month int `parquet:"month"`
}

const dateFormat = "20060102 15:04:05"

// fromFeedEntity reads a ProtoBuf VehiclePosition into a package-local VehiclePosition.
func (vp *VehiclePosition) fromFeedEntity(vehicle *gtfs.VehiclePosition, location *time.Location) error {
	trip := vehicle.GetTrip()
	position := vehicle.GetPosition()
	vehicleInfo := vehicle.GetVehicle()

	var startTime time.Time
	var err error
	if trip != nil {
		startTimeStr := trip.GetStartTime()
		startTime, err = time.ParseInLocation(dateFormat, trip.GetStartDate()+" "+startTimeStr, location)

		// If we encouter a >24h offset, we need to parse it separately and then add
		if err != nil && len(startTimeStr) > 3 {
			hourOffset, err := time.ParseDuration(startTimeStr[:2] + "h")
			if err != nil {
				return err
			}
			startTimeStr = "00" + startTimeStr[3:]
			startTime, err = time.ParseInLocation(dateFormat, trip.GetStartDate()+" "+startTimeStr, location)
			if err != nil {
				return err
			}
			startTime = startTime.Add(hourOffset)
		} else if err != nil {
			return err
		}
	}

	vp.TripId = trip.GetTripId()
	vp.RouteId = trip.GetRouteId()
	vp.DirectionId = int32(trip.GetDirectionId())
	vp.StartTimeUnix = startTime.Unix()
	vp.StartTime = startTime.UTC()
	vp.ScheduleRelationship = int32(trip.GetScheduleRelationship())
	vp.Latitude = position.GetLatitude()
	vp.Longitude = position.GetLongitude()
	vp.Bearing = position.GetBearing()
	vp.Odometer = position.GetOdometer()
	vp.Speed = position.GetSpeed()
	vp.CurrentStopSequence = vehicle.GetCurrentStopSequence()
	vp.StopId = vehicle.GetStopId()
	vp.CurrentStatus = int32(vehicle.GetCurrentStatus())
	// The GTFS protobuf is wrong on this, because Unix timestamps are allowed to be negative.
	// Thus it should be safe to truncate to the range of a signed int64.
	vp.TimestampUnix = int64(vehicle.GetTimestamp())
	vp.Timestamp = time.Unix(vp.TimestampUnix, 0).UTC()
	vp.CongestionLevel = int32(vehicle.GetCongestionLevel())
	vp.OccupancyStatus = int32(vehicle.GetOccupancyStatus())
	vp.VehicleId = vehicleInfo.GetId()
	vp.VehicleLabel = vehicleInfo.GetLabel()
	vp.LicensePlate = vehicleInfo.GetLicensePlate()

	return nil
}

// setupDatabase initializes and creates the realtime vehicle positions SQLite database.
func setupDatabase(dataDir string) *sqlx.DB {
	dbPath := filepath.Join(dataDir, "realtime.db")
	db := sqlx.MustOpen("sqlite3", dbPath)

	// Enabled for data integrity reasons
	db.MustExec("PRAGMA journal_mode=WAL")

	var query strings.Builder
	query.WriteString("CREATE TABLE IF NOT EXISTS vehicle_positions (")
	for _, colInfo := range columns {
		query.WriteString(colInfo.Name)
		query.WriteString(" ")
		query.WriteString(colInfo.Type)
		query.WriteString(",\n")
	}
	query.WriteString("PRIMARY KEY(timestamp, trip_id))")
	db.MustExec(query.String())
	return db
}

// addVehiclePositions inserts vehicle positions into a SQLite database.
// Timestamps from the feed are localized to the specified location.
func addVehiclePositions(feed *gtfs.FeedMessage, db *sqlx.DB, location *time.Location) error {
	tx := db.MustBegin()
	defer tx.Rollback()

	stmt, err := tx.PrepareNamed(insertQuery())
	if err != nil {
		return err
	}

	for _, entity := range feed.Entity {
		if entity.Vehicle == nil {
			continue
		}
		var vp VehiclePosition
		vp.fromFeedEntity(entity.Vehicle, location)
		// The BC Transit feed will occasionally publish entries with identical vehicle_ids and timestamps,
		// but a zero start_time and other trip-related fields missing.
		// Ignore these to avoid violating the primary key constraint.
		if vp.StartTime.IsZero() {
			continue
		}
		stmt.MustExec(&vp)
	}

	err = tx.Commit()
	if err != nil {
		return err
	}

	return nil
}

// extractFeed retrieves a GTFS feed from the specified URL and returns a FeedMessage.
// Returns an empty FeedMessage and error if extraction fails.
func extractFeed(feedURL string) (*gtfs.FeedMessage, error) {
	resp, err := http.Get(feedURL)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	feed := &gtfs.FeedMessage{}
	if err := proto.Unmarshal(data, feed); err != nil {
		return nil, err
	}
	return feed, nil
}
