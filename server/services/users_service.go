package services

import (
	"context"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"net"
	"path/filepath"
	"server/persistency"
	"server/proto"
	"strings"
)

type UserServer struct {
	proto.UnimplementedUserServiceServer
}

func (*UserServer) CreateUser(_ context.Context, request *proto.CreateUserRequest) (*proto.CreateUserResponse, error) {
	log.Debugf("Create user invoked with %v\n", request)

	user := request.GetUser()
	user.Username = strings.ToLower(user.Username)
	path := filepath.Join("User", user.Username)

	if persistency.FileExists(path) {
		return &proto.CreateUserResponse{Result: proto.OperationOutcome_FAILED}, status.Error(codes.AlreadyExists, "Username is taken")
	}

	err := persistency.Save(user, path)

	if err != nil {
		return &proto.CreateUserResponse{Result: proto.OperationOutcome_FAILED}, err
	}

	return &proto.CreateUserResponse{Result: proto.OperationOutcome_SUCCESS}, nil
}

func (*UserServer) GetUser(_ context.Context, request *proto.GetUserRequest) (*proto.GetUserResponse, error) {
	log.Debugf("Get user invoked with %v\n", request)

	username := request.GetUsername()
	user, err := persistency.Load[proto.User](filepath.Join("User", username))

	if err != nil {
		return nil, err
	}

	return &proto.GetUserResponse{User: &user}, nil
}

func (*UserServer) EditUser(_ context.Context, request *proto.EditUserRequest) (*proto.EditUserResponse, error) {
	log.Debugf("Edit user invoked with %v\n", request)

	user := request.GetUser()
	err := persistency.Save(user, filepath.Join("User", user.Username))

	if err != nil {
		return &proto.EditUserResponse{Result: proto.OperationOutcome_FAILED}, err
	}

	return &proto.EditUserResponse{Result: proto.OperationOutcome_SUCCESS}, nil
}

func StartUserService(network string, address string) {
	log.Infof("User Service Started\n")

	lis, err := net.Listen(network, address)

	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	s := grpc.NewServer()
	proto.RegisterUserServiceServer(s, &UserServer{})

	if err := s.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}

}
