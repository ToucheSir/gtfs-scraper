package main

import (
	"errors"
	"fmt"
	"log"
	"os"
	"path"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
	"github.com/parquet-go/parquet-go"
)

const yearMonthsQuery = `
	SELECT DISTINCT
		strftime('%Y', timestamp, 'unixepoch') AS year,
		strftime('%m', timestamp, 'unixepoch') AS month
	FROM vehicle_positions ORDER BY year, month
`

type PartitionKey struct{ Year, Month string }

func getDistinctYearMonths(db *sqlx.DB) ([]PartitionKey, error) {
	var yearMonths []PartitionKey
	err := db.Select(&yearMonths, yearMonthsQuery)
	return yearMonths, err
}

const partitionQuery = `
	SELECT
		trip_id,
		route_id,
		direction_id,
		CAST(start_time as INT) AS start_time,
		schedule_relationship,
		latitude,
		longitude,
		odometer,
		speed,
		current_stop_sequence,
		stop_id,
		current_status,
		CAST(timestamp AS INT) AS timestamp,
		congestion_level,
		occupancy_status,
		vehicle_id,
		vehicle_label,
		license_plate,
		strftime('%Y', timestamp, 'unixepoch') AS year,
		strftime('%m', timestamp, 'unixepoch') AS month
	FROM vehicle_positions WHERE year = ? AND month = ?
`

func queryPartition(db *sqlx.DB, key PartitionKey) (*sqlx.Rows, error) {
	rows, err := db.Queryx(partitionQuery, key.Year, key.Month)
	fmt.Printf("Creating partition for %v\n", key)
	return rows, err
}

const rowGroupSize = 1_000_000

func writePartition(archiveDir string, positions *sqlx.Rows, key PartitionKey) (err error) {
	partitionDir := path.Join(archiveDir, "year="+key.Year, "month="+key.Month)
	err = os.MkdirAll(partitionDir, 0775)
	if err != nil {
		return
	}

	f, err := os.Create(path.Join(partitionDir, "vehicle_positions.parquet"))
	if err != nil {
		return
	}
	writer := parquet.NewGenericWriter[VehiclePosition](f)
	defer func() {
		err = errors.Join(err, writer.Close())
	}()

	buffer := make([]VehiclePosition, 0, rowGroupSize)
	for positions.Next() {
		var vp VehiclePosition
		if err = positions.StructScan(&vp); err != nil {
			return
		}
		vp.StartTime = time.Unix(vp.StartTimeUnix, 0)
		vp.Timestamp = time.Unix(vp.TimestampUnix, 0)
		buffer = append(buffer, vp)
		if len(buffer) == rowGroupSize {
			n, err := writer.Write(buffer)
			if err != nil {
				return err
			} else if n != rowGroupSize {
				log.Panicf("Expected to write %d parquet rows, wrote %d", rowGroupSize, n)
			}
			if err := writer.Flush(); err != nil {
				return err
			}
			buffer = make([]VehiclePosition, 0, rowGroupSize)
		}
	}
	n, err := writer.Write(buffer)
	if err != nil {
		return err
	} else if n != len(buffer) {
		log.Panicf("Expected to write %d parquet rows, wrote %d", rowGroupSize, n)
	}
	return err
}

func archivePartitions(db *sqlx.DB, archiveDir string) error {
	yearMonths, err := getDistinctYearMonths(db)
	fmt.Println("Creating partitions:")
	fmt.Println(yearMonths)
	if err != nil {
		return err
	}
	for _, ym := range yearMonths {
		positions, err := queryPartition(db, ym)
		if err != nil {
			return err
		}
		if err := writePartition(archiveDir, positions, ym); err != nil {
			return err
		}
	}
	return nil
}
