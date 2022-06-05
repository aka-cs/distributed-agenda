package services

import (
	"context"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"net"
	"path/filepath"
	"server/persistency"
	"server/proto"
	"strconv"
)

type UserServer struct {
	proto.UnimplementedUserServiceServer
}

func (*UserServer) CreateUser(_ context.Context, request *proto.CreateUserRequest) (*proto.CreateUserResponse, error) {
	log.Debugf("Create user invoked with %v\n", request)

	user := request.GetUser()
	err := persistency.Save(user, filepath.Join("User", strconv.FormatInt(user.Id, 10)))

	if err != nil {
		return nil, err
	}

	return &proto.CreateUserResponse{Result: proto.OperationOutcome_SUCCESS}, nil
}

func (*UserServer) GetUser(_ context.Context, request *proto.GetUserRequest) (*proto.GetUserResponse, error) {
	log.Debugf("Get user invoked with %v\n", request)

	id := request.GetId()
	user, err := persistency.Load[proto.User](filepath.Join("User", strconv.FormatInt(id, 10)))

	if err != nil {
		return nil, err
	}

	return &proto.GetUserResponse{User: &user}, nil
}

func StartUserService() {
	log.Infof("User Service Started")

	lis, err := net.Listen("tcp", "0.0.0.0:50051")

	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	s := grpc.NewServer()
	proto.RegisterUserServiceServer(s, &UserServer{})

	if err := s.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}

}
