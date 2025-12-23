package main

import (
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
	"github.com/parquet-go/parquet-go"
)

// timestamp > 0 avoids the occasional row with no timestamp set (i.e. invalid data)
const archiveRangeQuery = `
	SELECT
		COALESCE(strftime('%Y-%m', MIN(timestamp), 'unixepoch'),'') AS min_ym,
		COALESCE(strftime('%Y-%m', MAX(timestamp), 'unixepoch'),'') AS max_ym
	FROM vehicle_positions where timestamp > 0
`

const yearMonthLayout = "2006-01"

func findArchiveRange(db *sqlx.DB) (startMonth time.Time, endMonth time.Time, err error) {
	var mm struct {
		MinYM string `db:"min_ym"`
		MaxYM string `db:"max_ym"`
	}
	err = db.Get(&mm, archiveRangeQuery)
	if err != nil || mm.MinYM == "" || mm.MaxYM == "" {
		return
	}

	startMonth, err = time.Parse(yearMonthLayout, mm.MinYM)
	if err != nil {
		return
	}
	endMonth, err = time.Parse(yearMonthLayout, mm.MaxYM)
	if err != nil {
		return
	}
	return startMonth, endMonth, nil
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
	FROM vehicle_positions WHERE timestamp >= ? AND timestamp < ?
`

func queryPartition(db *sqlx.DB, startTime time.Time, endTime time.Time) (*sqlx.Rows, error) {
	rows, err := db.Queryx(partitionQuery, startTime.Unix(), endTime.Unix())
	return rows, err
}

const rowGroupSize = 1_000_000

func findLastUpdates(reader *parquet.GenericReader[VehiclePosition], lastVehicleUpdates map[string]time.Time) error {
	buffer := make([]VehiclePosition, rowGroupSize)
	for {
		n, err := reader.Read(buffer)
		if errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return err
		}
		for _, vp := range buffer[:n] {
			if vp.VehicleId == "" {
				continue
			}
			if lastUpdate, found := lastVehicleUpdates[vp.VehicleId]; !found || vp.Timestamp.After(lastUpdate) {
				lastVehicleUpdates[vp.VehicleId] = vp.Timestamp
			}
		}
	}
	return nil
}

func writePartition(db *sqlx.DB, archiveDir string, period time.Time) (err error) {
	ym := period.Format(yearMonthLayout)
	partitionDir := filepath.Join(archiveDir, fmt.Sprintf("year=%04d", period.Year()), fmt.Sprintf("month=%02d", int(period.Month())))
	err = os.MkdirAll(partitionDir, 0775)
	if err != nil {
		return err
	}
	filePath := filepath.Join(partitionDir, "vehicle_positions.parquet")

	// Find last update times for each vehicle in existing file
	lastVehicleUpdates := make(map[string]time.Time)
	var oldReader *parquet.GenericReader[VehiclePosition]

	stagingPath := filePath
	oldFile, err := os.Open(filePath)
	if err == nil {
		defer oldFile.Close()
		oldReader = parquet.NewGenericReader[VehiclePosition](oldFile)
		defer oldReader.Close()

		log.Printf("%s: found %d rows in existing file\n", ym, oldReader.NumRows())
		if err := findLastUpdates(oldReader, lastVehicleUpdates); err != nil {
			return err
		}
		oldReader.Reset()
		log.Printf("%s: found updates for %d vehicles\n", ym, len(lastVehicleUpdates))
		stagingPath = filePath + ".tmp"
	} else if !errors.Is(err, os.ErrNotExist) {
		log.Panicln(err)
		return err
	}

	f, err := os.Create(stagingPath)
	if err != nil {
		return
	}
	writerConfig, err := parquet.NewWriterConfig(
		parquet.MaxRowsPerRowGroup(rowGroupSize),
		parquet.Compression(&parquet.Zstd),
	)
	if err != nil {
		return err
	}
	writer := parquet.NewGenericWriter[VehiclePosition](f, writerConfig)
	defer func() {
		err = errors.Join(err, writer.Close())
	}()

	if stagingPath != filePath {
		n, err := parquet.CopyRows(writer, oldReader)
		log.Printf("%s: copied %d rows from existing file\n", ym, n)
		if err != nil {
			return err
		} else if oldReader.NumRows() != n {
			log.Panicf("%s: expected to write %d parquet rows, wrote %d", ym, oldReader.NumRows(), n)
		}
	}

	// TODO work this into findLastUpdates?
	var minUpdateTime time.Time
	for _, t := range lastVehicleUpdates {
		if minUpdateTime.IsZero() || t.Before(minUpdateTime) {
			minUpdateTime = t
		}
	}
	if minUpdateTime.IsZero() {
		minUpdateTime = period
	}
	log.Printf("%s: querying data from %v to %v\n", ym, minUpdateTime, period.AddDate(0, 1, 0))
	positions, err := queryPartition(db, minUpdateTime, period.AddDate(0, 1, 0))
	if err != nil {
		log.Panicln(err)
		return err
	}
	buffer := make([]VehiclePosition, 0, rowGroupSize)
	var nNew int
	var nSkipped int
	var vp VehiclePosition
	for positions.Next() {
		if err = positions.StructScan(&vp); err != nil {
			return
		}
		vp.StartTime = time.Unix(vp.StartTimeUnix, 0)
		vp.Timestamp = time.Unix(vp.TimestampUnix, 0)

		// Don't append duplicate rows to existing files
		if lastUpdate, found := lastVehicleUpdates[vp.VehicleId]; found && !vp.Timestamp.After(lastUpdate) {
			nSkipped++
			continue
		}
		nNew++
		buffer = append(buffer, vp)
		if len(buffer) >= rowGroupSize {
			n, err := writer.Write(buffer)
			if err != nil {
				return err
			} else if n != rowGroupSize {
				log.Panicf("Expected to write %d parquet rows, wrote %d", rowGroupSize, n)
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
	log.Printf("%s: wrote %d new rows, skipped %d rows\n", ym, nNew, nSkipped)

	// Replace original file (this is probably non-atomic!)
	return os.Rename(stagingPath, filePath)
}

func archivePartitions(db *sqlx.DB, archiveDir string) error {
	if absPath, err := filepath.Abs(archiveDir); err == nil {
		log.Println("Archiving to", absPath, "...")
	}
	startMonth, endMonth, err := findArchiveRange(db)
	if err != nil {
		return err
	}
	log.Println("Creating partitions from", startMonth, "to", endMonth)
	for period := startMonth; !period.After(endMonth); period = period.AddDate(0, 1, 0) {
		log.Println("Writing partition for", period)
		if err := writePartition(db, archiveDir, period); err != nil {
			log.Panicln(err)
			return err
		}
		log.Println("Created partition for", period)
	}
	return nil
}
