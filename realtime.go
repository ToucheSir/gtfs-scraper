package main

import (
	"database/sql"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"time"

	"github.com/MobilityData/gtfs-realtime-bindings/golang/gtfs"
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
	{Name: "longitude", Type: "REAL"},
	{Name: "bearing", Type: "REAL"},
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

// setupDatabase initializes and sets up the SQLite database for storing vehicle positions.
// It takes the data directory path as input and returns a pointer to the sql.DB object and an error, if any.
func setupDatabase(dataDir string) (*sql.DB, error) {
	dbPath := filepath.Join(dataDir, "realtime.db")
	db, err := sql.Open("sqlite3", dbPath)
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
func addVehiclePositions(feed *gtfs.FeedMessage, db *sql.DB, location *time.Location) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	var query strings.Builder
	query.WriteString("INSERT INTO vehicle_positions (")
	for _, colInfo := range columns[:len(columns)-1] {
		query.WriteString(colInfo.Name)
		query.WriteString(", ")
	}
	query.WriteString(columns[len(columns)-1].Name)
	query.WriteString(") VALUES (")
	for range columns[:len(columns)-1] {
		query.WriteString("?, ")
	}
	query.WriteString("?) ON CONFLICT DO NOTHING")
	stmt, err := tx.Prepare(query.String())
	if err != nil {
		return err
	}

	for _, entity := range feed.Entity {
		if entity.Vehicle == nil {
			continue
		}
		vehicle := entity.Vehicle
		trip := vehicle.GetTrip()
		position := vehicle.GetPosition()
		vehicleInfo := vehicle.GetVehicle()

		var startTime time.Time
		if trip != nil {
			startTime, err = time.ParseInLocation("20060102 15:04:05", trip.GetStartDate()+" "+trip.GetStartTime(), location)
		}
		if err != nil {
			return err
		}

		_, err = stmt.Exec(
			trip.GetTripId(),
			trip.GetRouteId(),
			trip.GetDirectionId(),
			startTime.Unix(),
			trip.GetScheduleRelationship(),
			position.GetLatitude(),
			position.GetLongitude(),
			position.GetBearing(),
			position.GetOdometer(),
			position.GetSpeed(),
			vehicle.GetCurrentStopSequence(),
			vehicle.GetStopId(),
			vehicle.GetCurrentStatus(),
			vehicle.GetTimestamp(),
			vehicle.GetCongestionLevel(),
			vehicle.GetOccupancyStatus(),
			vehicleInfo.GetId(),
			vehicleInfo.GetLabel(),
			vehicleInfo.GetLicensePlate(),
		)
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
