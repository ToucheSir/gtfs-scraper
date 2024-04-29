package main

import (
	"encoding/json"
	"log"
	"os"
	"path"
	"time"

	"slices"
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
	validCommands := []string{
		"static",
		"alerts",
		"tripupdates",
		"vehicleupdates",
	}

	command := "static"
	if len(os.Args) > 1 {
		command = os.Args[1]
	}
	if !slices.Contains(validCommands, command) {
		log.Fatalf("Invalid command: %s\n", command)
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
		err = os.Mkdir(staticDir, 0666)
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

	if command == "alerts" {
		feed, err := extractFeed(config.AlertsURL)
		if err != nil {
			log.Panicln(err)
		}
		log.Println(feed)
		log.Panicln("archiving alerts not implemented")
	} else if command == "tripupdates" {
		log.Panicln("archiving trip updates not implemented")
	} else if command == "vehicleupdates" {
		feed, err := extractFeed(config.VehicleUpdatesURL)
		if err != nil {
			log.Panicln(err)
		}
		addVehiclePositions(feed, db, timeZone)
	}

}
