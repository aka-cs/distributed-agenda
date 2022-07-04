package persistency

import (
	"errors"
	"os"
	"path/filepath"
	"server/chord"
	"strings"

	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

func Save[T protoreflect.ProtoMessage](node *chord.Node, object T, path string) error {
	fullPath := strings.ToLower(filepath.ToSlash(filepath.Join("resources", path+".bin")))

	log.Debugf("Saving file: %s", fullPath)

	data, err := proto.Marshal(object)

	if err != nil {
		log.Errorf("Error serializing object: %v", err)
		return status.Error(codes.Internal, "Error saving data")
	}

	err = node.SetKey(fullPath, data, "")

	if err != nil {
		log.Errorf("Error saving file")
		return status.Error(codes.Internal, "Error saving data")
	}

	return nil
}

func Load[T protoreflect.ProtoMessage](node *chord.Node, path string, result T) (T, error) {

	fullPath := strings.ToLower(filepath.ToSlash(filepath.Join("resources", path+".bin")))

	log.Debugf("Loading file: %s", fullPath)

	var empty T

	data, err := node.GetKey(fullPath, "")

	if err != nil {
		log.Errorf("Error getting file: %v", err)
		return empty, status.Errorf(codes.Internal, "")
	}

	err = proto.Unmarshal(data, result)

	if err != nil {
		log.Errorf("Error deserializing object: %v", err)
		return empty, status.Errorf(codes.Internal, "")
	}

	return result, nil
}

func Delete(node *chord.Node, path string) error {
	fullPath := strings.ToLower(filepath.ToSlash(filepath.Join("resources", path+".bin")))

	err := node.DeleteKey(fullPath, "")

	if err != nil {
		log.Errorf("Error deleting file: %v", err)
		return status.Error(codes.Internal, "Couldn't delete file")
	}
	return nil
}

func FileExists(node *chord.Node, path string) bool {
	fullPath := strings.ToLower(filepath.ToSlash(filepath.Join("resources", path+".bin")))

	log.Debugf("Checking if file exists: %s", fullPath)

	if _, err := node.GetKey(fullPath, ""); errors.Is(err, os.ErrNotExist) {
		return false
	}
	log.Debugf("File already exists: %v", fullPath)
	return true
}
