package persistency

import (
	"encoding/gob"
	log "github.com/sirupsen/logrus"
	"os"
	"path/filepath"
)

func Save[T any](object T, path string) error {

	fullPath := filepath.Join("resources", path+".bin")
	dir := filepath.Dir(fullPath)

	log.Infof(dir + "\n")

	err := os.MkdirAll(dir, os.ModePerm)

	if err != nil {
		log.Errorf("Couldn't create directory:\n%v\n", err)
		return err
	}

	dataFile, err := os.Create(fullPath)

	if err != nil {
		log.Errorf("Error creating file:\n%v\n", err)
		return err
	}
	dataEncoder := gob.NewEncoder(dataFile)

	err = dataEncoder.Encode(object)
	if err != nil {
		log.Errorf("Error serializing object:\n%v\n", err)
		return err
	}

	err = dataFile.Close()
	if err != nil {
		log.Errorf("Error closing file:\n%v\n", err)
		return err
	}
	return nil
}

func Load[T any](path string) (T, error) {

	var result T
	var empty T

	dataFile, err := os.Open(filepath.Join("resources", path+".bin"))

	if err != nil {
		log.Errorf("Error opening file:\n%v\n", err)
		return empty, err
	}

	dataDecoder := gob.NewDecoder(dataFile)
	err = dataDecoder.Decode(&result)

	if err != nil {
		log.Errorf("Error deserializing object:\n%v\n", err)
		return empty, err
	}

	err = dataFile.Close()

	if err != nil {
		log.Errorf("Error closing file:\n%v\n", err)
		return empty, err
	}

	return result, nil
}
