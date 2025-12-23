package main

import (
	"bytes"
	"crypto/sha1"
	"errors"
	"io"
	"log"
	"mime"
	"net/http"
	"os"
	"path/filepath"
)

func downloadStatic(outputDir string, url string) {
	resp, err := http.Get(url)
	if err != nil {
		log.Panicln(err)
	}
	defer resp.Body.Close()

	disposition := resp.Header.Get("Content-Disposition")
	if disposition == "" {
		return // TODO error
	}
	_, params, err := mime.ParseMediaType(disposition)
	if err != nil {
		log.Panicln(err)
	}
	filename := params["filename"]

	outputFilename := filepath.Join(outputDir, filepath.Clean(filename))
	file, err := os.OpenFile(outputFilename, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0666)
	if err == nil {
		nbtyes, cerr := io.Copy(file, resp.Body)
		if cerr != nil {
			log.Panicln(cerr)
		}
		if nbtyes != resp.ContentLength {
			log.Panicf("Downloaded %d bytes but expected %d\n", nbtyes, resp.ContentLength)
		}

		fileEntries, err := os.ReadDir(outputDir)
		if err != nil {
			log.Panicln(err)
		}
		if len(fileEntries) == 0 {
			return
		}

		// If there are existing files, check if file contents have changed.
		var oldModTimestamp int64
		var oldFilename string
		for _, fileEntry := range fileEntries {
			info, err := fileEntry.Info()
			if err != nil {
				log.Panicln(err)
			}
			modTimestamp := info.ModTime().Unix()
			if modTimestamp > oldModTimestamp {
				oldFilename = fileEntry.Name()
			}
		}

		oldFile, err := os.OpenFile(filepath.Join(outputDir, oldFilename), os.O_RDONLY, 0666)
		if err != nil {
			log.Panicln(err)
		}
		oldHash := sha1.New()
		_, cerr = io.Copy(oldHash, oldFile)
		if cerr != nil {
			log.Panicln(cerr)
		}
		newHash := sha1.New()
		_, cerr = io.Copy(newHash, file)
		if cerr != nil {
			log.Panicln(cerr)
		}
		// Clean up new file if contents are unchanged
		if bytes.Equal(oldHash.Sum(nil), newHash.Sum(nil)) {
			defer os.Remove(outputFilename)
		} else {
			log.Printf("Downloaded static GTFS data: %s\n", outputFilename)
		}
	} else if !errors.Is(err, os.ErrExist) {
		log.Panicln(err)
	}
	defer file.Close()
}
