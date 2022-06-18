package persistency

import (
	"encoding/gob"
	"errors"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"os"
	"path/filepath"
)

func Save[T any](object T, path string) error {
	fullPath := filepath.Join("resources", path+".bin")

	log.Debugf("Saving file: %s\n", fullPath)

	dir := filepath.Dir(fullPath)

	err := os.MkdirAll(dir, os.ModePerm)

	if err != nil {
		log.Errorf("Couldn't create directory:\n%v\n", err)
		return status.Error(codes.Internal, "Couldn't create directory")
	}

	dataFile, err := os.Create(fullPath)

	defer closeFile(dataFile)

	if err != nil {
		log.Errorf("Error creating file:\n%v\n", err)
		return status.Error(codes.Internal, "Couldn't create file")
	}
	dataEncoder := gob.NewEncoder(dataFile)

	err = dataEncoder.Encode(object)
	if err != nil {
		log.Errorf("Error serializing object:\n%v\n", err)
		return status.Error(codes.Internal, "Couldn't serialize object")
	}

	return nil
}

func Load[T any](path string) (T, error) {

	fullPath := filepath.Join("resources", path+".bin")

	log.Debugf("Loading file: %s\n", fullPath)

	var result T
	var empty T

	dataFile, err := os.Open(fullPath)
	defer closeFile(dataFile)

	if err != nil {
		log.Errorf("Error opening file:\n%v\n", err)
		return empty, status.Error(codes.Internal, "Couldn't open file")
	}

	dataDecoder := gob.NewDecoder(dataFile)
	err = dataDecoder.Decode(&result)

	if err != nil {
		log.Errorf("Error deserializing object:\n%v\n", err)
		return empty, status.Error(codes.Internal, "Couldn't deserialize file")
	}

	return result, nil
}

func Delete(path string) error {
	err := os.Remove(filepath.Join("resources", path+".bin"))

	if err != nil {
		log.Errorf("Error deleting file:\n%v\n", err)
		return status.Error(codes.Internal, "Couldn't delete file")
	}
	return nil
}

func FileExists(path string) bool {
	fullPath := filepath.Join("resources", path+".bin")
	if _, err := os.Stat(fullPath); errors.Is(err, os.ErrNotExist) {
		return false
	}
	log.Debugf("File already exists: %v\n", fullPath)
	return true
}

func closeFile(file *os.File) {
	err := file.Close()
	if err != nil {
		log.Errorf("Error closing file:\n%v\n", err)
	}
}
