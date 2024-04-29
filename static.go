package main

import (
	"bytes"
	"cmp"
	"crypto/sha1"
	"errors"
	"io"
	"io/fs"
	"log"
	"mime"
	"net/http"
	"os"
	"path"
	"slices"
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

	outputFilename := path.Join(outputDir, path.Clean(filename))
	file, err := os.OpenFile(outputFilename, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0666)
	if err == nil {
		fileEntries, err := os.ReadDir(outputDir)
		if err != nil {
			log.Panicln(err)
		}
		oldFileInfo := slices.MaxFunc(fileEntries, func(a, b fs.DirEntry) int {
			infoA, errA := a.Info()
			infoB, errB := b.Info()
			if errA != nil || errB != nil {
				log.Panicf("Error reading file info: %v %v\n", errA, errB)
			}
			return cmp.Compare(infoA.ModTime().Unix(), infoB.ModTime().Unix())
		})
		oldFilename := oldFileInfo.Name()

		nbtyes, cerr := io.Copy(file, resp.Body)
		if cerr != nil {
			log.Panicln(cerr)
		}
		if nbtyes != resp.ContentLength {
			log.Panicf("Downloaded %d bytes but expected %d\n", nbtyes, resp.ContentLength)
		}

		// Check if file contents have changed.
		oldFile, err := os.OpenFile(path.Join(outputDir, oldFilename), os.O_RDONLY, 0666)
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
