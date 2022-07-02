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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type HistoryServer struct {
	proto.UnimplementedHistoryServiceServer
}

func (*HistoryServer) AddHistoryEntry(_ context.Context, request *proto.AddHistoryEntryRequest) (*proto.AddHistoryEntryResponse, error) {

	entry := request.GetEntry()
	users := request.GetUsers()

	for i := 0; i < len(users); i++ {
		path := filepath.Join("History", users[i])
		history, err := persistency.Load[[]proto.HistoryEntry](path)
		if err != nil {
			return &proto.AddHistoryEntryResponse{}, err
		}
		history = append(history, *entry)
		err = persistency.Save(history, path)

		if err != nil {
			return &proto.AddHistoryEntryResponse{}, err
		}
	}
	return &proto.AddHistoryEntryResponse{}, nil
}

func (*HistoryServer) GetFullHistory(request *proto.GetFullHistoryRequest, stream proto.HistoryService_GetFullHistoryServer) error {

	username := request.GetUsername()
	path := filepath.Join("History", username)

	history, err := persistency.Load[[]proto.HistoryEntry](path)
	if err != nil {
		return err
	}

	for i := 0; i < len(history); i++ {
		err = stream.Send(&proto.GetFullHistoryResponse{
			Entry: &history[i],
		})

		if err != nil {
			log.Errorf("Error sending response GetFullHistory:\n%v\n", err)
			return status.Error(codes.Internal, "Error sending response")
		}
	}
	return nil
}

func (*HistoryServer) GetHistoryFromOffset(request *proto.GetHistoryFromOffsetRequest, stream proto.HistoryService_GetHistoryFromOffsetServer) error {

	username := request.GetUsername()
	offset := int(request.GetOffset())

	path := filepath.Join("History", username)

	history, err := persistency.Load[[]proto.HistoryEntry](path)
	if err != nil {
		return err
	}

	for i := offset; i < len(history); i++ {
		err = stream.Send(&proto.GetHistoryFromOffsetResponse{
			Entry: &history[i],
		})

		if err != nil {
			log.Errorf("Error sending response GetHistoryFromOffset:\n%v\n", err)
			return status.Error(codes.Internal, "Error sending response")
		}
	}
	return nil
}

func StartHistoryService(network string, address string) {
	log.Infof("History Service Started\n")

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

	proto.RegisterHistoryServiceServer(s, &HistoryServer{})

	if err := s.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}
