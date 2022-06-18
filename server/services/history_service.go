package services

import (
	"context"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"path/filepath"
	"server/persistency"
	"server/proto"
)

type HistoryServer struct {
	proto.UnimplementedHistoryServiceServer
}

func (*HistoryServer) AddHistoryEntry(_ context.Context, request *proto.AddHistoryEntryRequest) (*proto.AddHistoryEntryResponse, error) {
	log.Debugf("Add History Entry invoked with %v\n", request)

	entry := request.GetEntry()
	users := request.GetUsers()

	for i := 0; i < len(users); i++ {
		path := filepath.Join("History", users[i])
		history, err := persistency.Load[[]proto.HistoryEntry](path)
		if err != nil {
			return &proto.AddHistoryEntryResponse{Result: proto.OperationOutcome_FAILED}, err
		}
		history = append(history, *entry)
		err = persistency.Save(history, path)

		if err != nil {
			return &proto.AddHistoryEntryResponse{Result: proto.OperationOutcome_FAILED}, err
		}
	}
	return &proto.AddHistoryEntryResponse{Result: proto.OperationOutcome_SUCCESS}, nil
}

func (*HistoryServer) GetFullHistory(request *proto.GetFullHistoryRequest, stream proto.HistoryService_GetFullHistoryServer) error {
	log.Debugf("Get Full History invoked with %v\n", request)

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
	log.Debugf("Get History From Offset invoked with %v\n", request)

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
