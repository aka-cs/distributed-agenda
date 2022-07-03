package services

import (
	"context"
	"net"
	"path/filepath"
	"server/persistency"
	"server/proto"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

type UserServer struct {
	proto.UnimplementedUserServiceServer
}

func (*UserServer) GetUser(_ context.Context, request *proto.GetUserRequest) (*proto.GetUserResponse, error) {
	username := request.GetUsername()
	user, err := persistency.Load[proto.User](&node, filepath.Join("User", username))

	if err != nil {
		return nil, err
	}

	user.PasswordHash = ""

	return &proto.GetUserResponse{User: &user}, nil
}

func (*UserServer) EditUser(_ context.Context, request *proto.EditUserRequest) (*proto.EditUserResponse, error) {
	user := request.GetUser()
	err := persistency.Save(&node, user, filepath.Join("User", user.Username))

	if err != nil {
		return nil, err
	}

	return &proto.EditUserResponse{}, nil
}

func StartUserService(network string, address string) {
	log.Infof("User Service Started")

	lis, err := net.Listen(network, address)

	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	s := grpc.NewServer(
		grpc.UnaryInterceptor(
			grpc_middleware.ChainUnaryServer(
				UnaryLoggingInterceptor,
				UnaryServerInterceptor,
			),
		), grpc.StreamInterceptor(
			grpc_middleware.ChainStreamServer(
				StreamLoggingInterceptor,
				StreamServerInterceptor,
			),
		),
	)

	proto.RegisterUserServiceServer(s, &UserServer{})

	if err := s.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}

}
