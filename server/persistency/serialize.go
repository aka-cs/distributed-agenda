package persistency

import (
	"encoding/gob"
	"errors"
	"os"
	"path/filepath"

	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func Save[T any](object T, path string) error {
	fullPath := filepath.Join("resources", path+".bin")

	log.Debugf("Saving file: %s", fullPath)

	dir := filepath.Dir(fullPath)

	err := os.MkdirAll(dir, os.ModePerm)

	if err != nil {
		log.Errorf("Couldn't create directory: %v", err)
		return status.Error(codes.Internal, "Couldn't create directory")
	}

	dataFile, err := os.Create(fullPath)

	defer closeFile(dataFile)

	if err != nil {
		log.Errorf("Error creating file: %v", err)
		return status.Error(codes.Internal, "Couldn't create file")
	}
	dataEncoder := gob.NewEncoder(dataFile)

	err = dataEncoder.Encode(object)
	if err != nil {
		log.Errorf("Error serializing object: %v", err)
		return status.Error(codes.Internal, "Couldn't serialize object")
	}

	return nil
}

func Load[T any](path string) (T, error) {

	base := filepath.Base(path)

	fullPath := filepath.Join("resources", path+".bin")

	log.Debugf("Loading file: %s", fullPath)

	var result T
	var empty T

	dataFile, err := os.Open(fullPath)

	if err != nil {
		log.Errorf("Error opening file: %v", err)
		return empty, status.Errorf(codes.NotFound, "{} not found", base)
	}

	defer closeFile(dataFile)

	dataDecoder := gob.NewDecoder(dataFile)
	err = dataDecoder.Decode(&result)

	if err != nil {
		log.Errorf("Error deserializing object: %v", err)
		return empty, status.Errorf(codes.Internal, "Error returning {}", base)
	}

	return result, nil
}

func Delete(path string) error {
	err := os.Remove(filepath.Join("resources", path+".bin"))

	if err != nil {
		log.Errorf("Error deleting file: %v", err)
		return status.Error(codes.Internal, "Couldn't delete file")
	}
	return nil
}

func FileExists(path string) bool {
	fullPath := filepath.Join("resources", path+".bin")
	if _, err := os.Stat(fullPath); errors.Is(err, os.ErrNotExist) {
		return false
	}
	log.Debugf("File already exists: %v", fullPath)
	return true
}

func closeFile(file *os.File) {
	err := file.Close()
	if err != nil {
		log.Errorf("Error closing file: %v", err)
	}
}
