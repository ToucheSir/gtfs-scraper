package main

import (
	"database/sql"
	"io"
	"net/http"
	"path/filepath"
	"time"

	"github.com/MobilityData/gtfs-realtime-bindings/golang/gtfs"
	_ "github.com/mattn/go-sqlite3"
	"google.golang.org/protobuf/proto"
)

// setupDatabase initializes and sets up the SQLite database for storing vehicle positions.
// It takes the data directory path as input and returns a pointer to the sql.DB object and an error, if any.
func setupDatabase(dataDir string) (*sql.DB, error) {
	dbPath := filepath.Join(dataDir, "realtime.db")
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, err
	}

	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS vehicle_positions (
			trip_id TEXT,
			start_time DATETIME,
			schedule_relationship INT8,
			latitude REAL,
			longitude REAL,
			bearing REAL,
			odometer REAL,
			speed REAL,
			current_stop_sequence INTEGER,
			stop_id TEXT,
			current_status INT8,
			timestamp DATETIME,
			congestion_level INT8,
			occupancy_status INT8,
			vehicle_id TEXT,
			vehicle_label TEXT,
			license_plate TEXT,
			UNIQUE(trip_id, timestamp)
		)
	`)
	if err != nil {
		return nil, err
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

	stmt, err := tx.Prepare(`
		INSERT INTO vehicle_positions (
			trip_id,
			start_time,
			schedule_relationship,
			latitude,
			longitude,
			bearing,
			odometer,
			speed,
			current_stop_sequence,
			stop_id,
			current_status,
			timestamp,
			congestion_level,
			occupancy_status,
			vehicle_id,
			vehicle_label,
			license_plate
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT DO NOTHING
	`)
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
		startTime, err := time.ParseInLocation("20060102 15:04:05", trip.GetStartDate()+" "+trip.GetStartTime(), &location)
		if err != nil {
			return err
		}

		_, err = stmt.Exec(
			trip.GetTripId(),
			startTime,
			trip.GetScheduleRelationship(),
			position.GetLatitude(),
			position.GetLongitude(),
			position.GetBearing(),
			position.GetOdometer(),
			position.GetSpeed(),
			vehicle.GetCurrentStopSequence(),
			vehicle.GetStopId(),
			vehicle.GetCurrentStatus(),
			time.Unix(int64(vehicle.GetTimestamp()), 0),
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
