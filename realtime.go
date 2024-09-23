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
	for _, colInfo := range columns[:len(columns)-1] {
		query.WriteString(colInfo.Name)
		query.WriteString(", ")
	}
	query.WriteString(columns[len(columns)-1].Name)
	query.WriteString(") VALUES (")
	for _, colInfo := range columns[:len(columns)-1] {
		query.WriteByte(':')
		query.WriteString(colInfo.Name)
		query.WriteByte(',')
	}
	query.WriteByte(':')
	query.WriteString(columns[len(columns)-1].Name)
	query.WriteString(") ON CONFLICT DO NOTHING")

	return query.String()
}

type VehiclePosition struct {
	TripId      string `db:"trip_id" parquet:"trip_id"`
	RouteId     string `db:"route_id" parquet:"route_id"`
	DirectionId byte   `db:"direction_id" parquet:"direction_id"`
	// Need to have two different fields for (de)serizialization from ProtoBuf -> SQLite -> Parquet.
	// The Go SQLite driver force converts time.Time to TEXT, so we must use an int column instead.
	// Parquet, however, does treat it as a 8-byte timestamp.
	StartTime            time.Time `db:"-" parquet:"start_time"`
	StartTimeUnix        int64     `db:"start_time" parquet:"-"`
	ScheduleRelationship byte      `db:"schedule_relationship" parquet:"schedule_relationship"`
	Latitude             float32   `db:"latitude" parquet:"latitude"`
	Longitude            float32   `db:"longitude" parquet:"longitude"`
	Bearing              float32   `db:"bearing" parquet:"bearing"`
	Odometer             float64   `db:"odometer" parquet:"odometer"`
	Speed                float32   `db:"speed" parquet:"speed"`
	CurrentStopSequence  uint32    `db:"current_stop_sequence" parquet:"current_stop_sequence"`
	StopId               string    `db:"stop_id" parquet:"stop_id"`
	CurrentStatus        byte      `db:"current_status" parquet:"current_status"`
	// Same treatment applies here
	Timestamp       time.Time `db:"-" parquet:"timestamp"`
	TimestampUnix   int64     `db:"timestamp" parquet:"-"`
	CongestionLevel byte      `db:"congestion_level" parquet:"congestion_level"`
	OccupancyStatus byte      `db:"occupancy_status" parquet:"occupancy_status"`
	VehicleId       string    `db:"vehicle_id" parquet:"vehicle_id"`
	VehicleLabel    string    `db:"vehicle_label" parquet:"vehicle_label"`
	LicensePlate    string    `db:"license_plate" parquet:"license_plate"`
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
	vp.DirectionId = byte(trip.GetDirectionId())
	vp.StartTime = startTime
	vp.StartTimeUnix = startTime.Unix()
	vp.ScheduleRelationship = byte(trip.GetScheduleRelationship())
	vp.Latitude = position.GetLatitude()
	vp.Longitude = position.GetLongitude()
	vp.Bearing = position.GetBearing()
	vp.Odometer = position.GetOdometer()
	vp.Speed = position.GetSpeed()
	vp.CurrentStopSequence = vehicle.GetCurrentStopSequence()
	vp.StopId = vehicle.GetStopId()
	vp.CurrentStatus = byte(vehicle.GetCurrentStatus())
	// The GTFS protobuf is wrong on this, because Unix timestamps are allowed to be negative.
	// Thus it should be safe to truncate to the range of a signed int64.
	vp.TimestampUnix = int64(vehicle.GetTimestamp())
	vp.Timestamp = time.Unix(vp.TimestampUnix, 0)
	vp.CongestionLevel = byte(vehicle.GetCongestionLevel())
	vp.OccupancyStatus = byte(vehicle.GetOccupancyStatus())
	vp.VehicleId = vehicleInfo.GetId()
	vp.VehicleLabel = vehicleInfo.GetLabel()
	vp.LicensePlate = vehicleInfo.GetLicensePlate()

	return nil
}

// setupDatabase initializes and sets up the SQLite database for storing vehicle positions.
// It takes the data directory path as input and returns a pointer to the sql.DB object and an error, if any.
func setupDatabase(dataDir string) (*sqlx.DB, error) {
	dbPath := filepath.Join(dataDir, "realtime.db")
	db, err := sqlx.Open("sqlite3", dbPath)
	if err != nil {
		return nil, err
	}

	// Enabled for data integrity reasons
	_, err = db.Exec("PRAGMA journal_mode=WAL")
	if err != nil {
		return db, err
	}

	var query strings.Builder
	query.WriteString("CREATE TABLE IF NOT EXISTS vehicle_positions (")
	for _, colInfo := range columns {
		query.WriteString(colInfo.Name)
		query.WriteString(" ")
		query.WriteString(colInfo.Type)
		query.WriteString(",\n")
	}
	query.WriteString("UNIQUE(trip_id, timestamp))")
	_, err = db.Exec(query.String())
	if err != nil {
		return db, err
	}
	return db, nil
}

// addVehiclePositions inserts vehicle positions into a SQLite database.
// It takes a feed URL, a data directory path, and a time.Location as input.
func addVehiclePositions(feed *gtfs.FeedMessage, db *sqlx.DB, location *time.Location) error {
	tx := db.MustBegin()
	defer tx.Rollback()

	stmt, err := tx.Prepare(insertQuery())
	if err != nil {
		return err
	}

	for _, entity := range feed.Entity {
		if entity.Vehicle == nil {
			continue
		}
		var vp VehiclePosition
		vp.fromFeedEntity(entity.Vehicle, location)

		_, err = stmt.Exec(&vp)
		if err != nil {
			return err
		}
	}

	err = tx.Commit()
	if err != nil {
		return err
	}

	return nil
}

// extractFeed retrieves a GTFS feed from the specified URL and returns a FeedMessage.
// It makes an HTTP GET request to the feedURL, reads the response body, and unmarshals
// the data into a FeedMessage struct using protocol buffers.
// If any error occurs during the process, it returns an empty FeedMessage and the error.
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
