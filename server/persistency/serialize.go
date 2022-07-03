package persistency

import (
	"bufio"
	"bytes"
	"encoding/gob"
	"errors"
	"os"
	"path/filepath"
	"server/chord"

	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func Save[T any](node *chord.Node, object T, path string) error {
	fullPath := filepath.ToSlash(filepath.Join("resources", path+".bin"))

	log.Debugf("Saving file: %s", fullPath)

	var buffer bytes.Buffer
	ioWriter := bufio.NewWriter(&buffer)

	dataEncoder := gob.NewEncoder(ioWriter)

	err := dataEncoder.Encode(object)

	if err != nil {
		log.Errorf("Error serializing object: %v", err)
		return status.Error(codes.Internal, "Error saving data")
	}

	err = ioWriter.Flush()
	if err != nil {
		log.Errorf("Error flushing buffer")
		return status.Error(codes.Internal, "Error saving data")
	}

	err = node.SetFile(fullPath, buffer.Bytes())

	if err != nil {
		log.Errorf("Error saving file")
		return status.Error(codes.Internal, "Error saving data")
	}

	return nil
}

func Load[T any](node *chord.Node, path string) (T, error) {

	fullPath := filepath.ToSlash(filepath.Join("resources", path+".bin"))

	log.Debugf("Loading file: %s", fullPath)

	var result T
	var empty T

	var buffer bytes.Buffer

	data, err := node.GetFile(fullPath)

	if err != nil {
		log.Errorf("Error getting file: %v", err)
		return empty, status.Errorf(codes.Internal, "")
	}

	buffer.Write(data)

	ioReader := bufio.NewReader(&buffer)

	dataDecoder := gob.NewDecoder(ioReader)
	err = dataDecoder.Decode(&result)

	if err != nil {
		log.Errorf("Error deserializing object: %v", err)
		return empty, status.Errorf(codes.Internal, "")
	}

	return result, nil
}

func Delete(node *chord.Node, path string) error {
	fullPath := filepath.ToSlash(filepath.Join("resources", path+".bin"))

	err := node.DeleteFile(fullPath)

	if err != nil {
		log.Errorf("Error deleting file: %v", err)
		return status.Error(codes.Internal, "Couldn't delete file")
	}
	return nil
}

func FileExists(node *chord.Node, path string) bool {
	fullPath := filepath.ToSlash(filepath.Join("resources", path+".bin"))

	log.Debugf("Checking if file exists: %s", fullPath)

	if _, err := node.GetFile(fullPath); errors.Is(err, os.ErrNotExist) {
		return false
	}
	log.Debugf("File already exists: %v", fullPath)
	return true
}
