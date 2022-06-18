package services

import (
	"context"
	log "github.com/sirupsen/logrus"
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

func (*HistoryServer) GetHistory(request *proto.GetHistoryRequest, stream proto.HistoryService_GetHistoryServer) error {
	log.Debugf("Get History invoked with %v\n", request)

	username := request.GetUsername()
	path := filepath.Join("History", username)

	history, err := persistency.Load[[]proto.HistoryEntry](path)
	if err != nil {
		return err
	}

	for i := 0; i < len(history); i++ {
		err = stream.Send(&proto.GetHistoryResponse{
			Entry: &history[i],
		})

		if err != nil {
			log.Errorf("Error sending response GetHistory:\n%v\n", err)
			return err
		}
	}
	return nil
}
