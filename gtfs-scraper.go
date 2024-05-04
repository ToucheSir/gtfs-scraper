package main

import (
	"encoding/json"
	"log"
	"os"
	"path"
	"time"
)

type Config struct {
	DataDir           string
	StaticURL         string
	AlertsURL         string
	TripUpdatesURL    string
	VehicleUpdatesURL string
	TimeZone          string
}

func main() {
	command := "static"
	if len(os.Args) > 1 {
		command = os.Args[1]
	}

	contents, err := os.ReadFile("gtfs-scraper.json")
	if err != nil {
		log.Panicln(err)
	}

	var config Config
	err = json.Unmarshal(contents, &config)
	if err != nil {
		log.Panicln(err)
	}

	if command == "static" {
		staticDir := path.Join(config.DataDir, "static")
		err = os.Mkdir(staticDir, 0775)
		if err != nil && !os.IsExist(err) {
			log.Panicln(err)
		}
		downloadStatic(staticDir, config.StaticURL)
		return
	}

	db, err := setupDatabase(config.DataDir)
	if err != nil {
		log.Panicln(err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			log.Panicln(err)
		}
	}()
	timeZone, err := time.LoadLocation(config.TimeZone)
	if err != nil {
		log.Panicln(err)
	}

	switch command {
	case "alerts":
		feed, err := extractFeed(config.AlertsURL)
		if err != nil {
			log.Panicln(err)
		}
		log.Println(feed)
		log.Panicln("archiving alerts not implemented")
	case "tripupdates":
		log.Panicln("archiving trip updates not implemented")
	case "vehicleupdates":
		feed, err := extractFeed(config.VehicleUpdatesURL)
		if err != nil {
			log.Panicln(err)
		}
		err = addVehiclePositions(feed, db, timeZone)
		if err != nil {
			log.Panicln(err)
		}
	default:
		log.Panicf("Invalid command: %s\n", command)
	}
}
