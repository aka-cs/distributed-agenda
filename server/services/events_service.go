package services

import (
	"context"
	"net"
	"path/filepath"
	"server/persistency"
	"server/proto"
	"strconv"

	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

type EventsServer struct {
	proto.UnimplementedEventsServiceServer
}

func (*EventsServer) GetEvent(_ context.Context, request *proto.GetEventRequest) (*proto.GetEventResponse, error) {
	log.Debugf("Get event invoked with %v\n", request)

	id := request.GetId()
	event, err := persistency.Load[proto.Event](filepath.Join("Event", strconv.FormatInt(id, 10)))

	if err != nil {
		return nil, err
	}

	return &proto.GetEventResponse{Event: &event}, nil
}

func (*EventsServer) CreateEvent(_ context.Context, request *proto.CreateEventRequest) (*proto.CreateEventResponse, error) {
	log.Debugf("Create event invoked with %v\n", request)

	event := request.GetEvent()
	err := persistency.Save(event, filepath.Join("Event", strconv.FormatInt(event.Id, 10)))

	if err != nil {
		return &proto.CreateEventResponse{Result: proto.OperationOutcome_FAILED}, err
	}

	return &proto.CreateEventResponse{Result: proto.OperationOutcome_SUCCESS}, nil
}

func (*EventsServer) DeleteEvent(_ context.Context, request *proto.DeleteEventRequest) (*proto.DeleteEventResponse, error) {
	log.Debugf("Delete Event invoked with %v\n", request)

	id := request.GetId()
	err := persistency.Delete(filepath.Join("Group", strconv.FormatInt(id, 10)))

	if err != nil {
		return &proto.DeleteEventResponse{Result: proto.OperationOutcome_FAILED}, err
	}

	return &proto.DeleteEventResponse{Result: proto.OperationOutcome_SUCCESS}, nil
}

func StartEventService(network string, address string) {
	log.Infof("Event Service Started\n")

	lis, err := net.Listen(network, address)

	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	s := grpc.NewServer(grpc.UnaryInterceptor(UnaryServerInterceptor), grpc.StreamInterceptor(StreamServerInterceptor))

	proto.RegisterEventsServiceServer(s, &EventsServer{})

	if err := s.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}
