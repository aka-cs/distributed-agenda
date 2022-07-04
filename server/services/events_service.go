package services

import (
	"context"
	"math/rand"
	"net"
	"path/filepath"
	"server/persistency"
	"server/proto"
	"strconv"
	"time"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type EventsServer struct {
	proto.UnimplementedEventsServiceServer
}

func (*EventsServer) GetEvent(_ context.Context, request *proto.GetEventRequest) (*proto.GetEventResponse, error) {

	id := request.GetId()
	event := &proto.Event{}
	event, err := persistency.Load(node, filepath.Join("Event", strconv.FormatInt(id, 10)), event)

	if err != nil {
		return nil, err
	}

	return &proto.GetEventResponse{Event: event}, nil
}

func (*EventsServer) CreateEvent(ctx context.Context, request *proto.CreateEventRequest) (*proto.CreateEventResponse, error) {

	username, err := getUsernameFromContext(ctx)

	if err != nil {
		return &proto.CreateEventResponse{}, err
	}

	event := request.GetEvent()

	event.Draft = false

	seed := rand.NewSource(time.Now().UnixNano())
	generator := rand.New(seed)
	event.Id = generator.Int63()

	users := proto.UserList{Users: make([]string, 0)}

	groupId := request.GetEvent().GetGroupId()

	if groupId != 0 {
		group := &proto.Group{}
		group, err := persistency.Load(node, filepath.Join("Group", strconv.FormatInt(groupId, 10)), group)

		if err != nil {
			return &proto.CreateEventResponse{}, err
		}
		usernames, err := getGroupUsernames(group)

		if err != nil {
			return &proto.CreateEventResponse{}, err
		}

		users.Users = append(users.Users, usernames...)

		hierarchy, err := hasHierarchy(group)

		if err != nil {
			return &proto.CreateEventResponse{}, err
		}

		if !hierarchy {
			event.Draft = true
		}

	} else {
		users.Users = append(users.Users, username)
	}

	invalids, err := checkValid(event, users.Users)

	if err != nil {
		return &proto.CreateEventResponse{Unavailable: invalids}, err
	}

	err = persistency.Save(node, event, filepath.Join("Event", strconv.FormatInt(event.Id, 10)))

	if err != nil {
		return &proto.CreateEventResponse{}, err
	}

	err = persistency.Save(node, &users, filepath.Join("EventParticipants", strconv.FormatInt(event.Id, 10)))
	if err != nil {
		return &proto.CreateEventResponse{}, err
	}

	updateEventHistory(ctx, proto.Action_CREATE, event, users.Users)

	confirmations := proto.Confirmations{Confirmations: map[string]bool{}}

	for i := range users.Users {
		confirmations.Confirmations[users.Users[i]] = false
	}

	confirmations.Confirmations[username] = true

	err = persistency.Save(node, &confirmations, filepath.Join("EventConfirmations", strconv.FormatInt(event.Id, 10)))

	if err != nil {
		return &proto.CreateEventResponse{}, err
	}

	return &proto.CreateEventResponse{}, nil
}

func (*EventsServer) DeleteEvent(ctx context.Context, request *proto.DeleteEventRequest) (*proto.DeleteEventResponse, error) {

	id := request.GetId()

	path := filepath.Join("Event", strconv.FormatInt(id, 10))

	event := &proto.Event{}
	event, err := persistency.Load(node, path, event)

	if err != nil {
		return &proto.DeleteEventResponse{}, err
	}

	err = persistency.Delete(node, path)

	if err != nil {
		return &proto.DeleteEventResponse{}, err
	}

	ppath := filepath.Join("EventParticipants", strconv.FormatInt(event.Id, 10))

	members := &proto.UserList{}
	members, err = persistency.Load(node, ppath, members)

	if err != nil {
		return &proto.DeleteEventResponse{}, err
	}

	err = persistency.Delete(node, ppath)

	if err != nil {
		return &proto.DeleteEventResponse{}, err
	}

	err = updateEventHistory(ctx, proto.Action_DELETE, event, members.Users)

	if err != nil {
		return &proto.DeleteEventResponse{}, err
	}

	return &proto.DeleteEventResponse{}, nil
}

func (*EventsServer) ConfirmEvent(ctx context.Context, request *proto.ConfirmEventRequest) (*proto.ConfirmEventResponse, error) {

	username, err := getUsernameFromContext(ctx)

	if err != nil {
		return &proto.ConfirmEventResponse{}, err
	}

	path := filepath.Join("EventConfirmations", strconv.FormatInt(request.GetEventId(), 10))

	confirmations := &proto.Confirmations{}
	confirmations, err = persistency.Load(node, path, confirmations)

	if err != nil {
		return &proto.ConfirmEventResponse{}, err
	}

	if _, ok := confirmations.Confirmations[username]; !ok {
		return &proto.ConfirmEventResponse{}, status.Error(codes.PermissionDenied, "")
	}

	confirmations.Confirmations[username] = true

	err = persistency.Save(node, confirmations, path)

	if err != nil {
		return &proto.ConfirmEventResponse{}, err
	}

	path = filepath.Join("Event", strconv.FormatInt(request.GetEventId(), 10))

	event := &proto.Event{}
	event, err = persistency.Load(node, path, event)

	updateEventHistory(ctx, proto.Action_CONFIRM, event, []string{username})

	users := make([]string, 0)

	for key := range confirmations.Confirmations {
		if !confirmations.Confirmations[key] {
			return &proto.ConfirmEventResponse{}, nil
		}
		users = append(users, key)
	}

	if err != nil {
		return &proto.ConfirmEventResponse{}, err
	}

	event.Draft = true

	err = persistency.Save(node, event, path)

	if err != nil {
		return &proto.ConfirmEventResponse{}, err
	}

	updateEventHistory(ctx, proto.Action_UPDATE, event, users)

	return &proto.ConfirmEventResponse{}, nil
}

func (*EventsServer) RejectEvent(ctx context.Context, request *proto.RejectEventRequest) (*proto.RejectEventResponse, error) {

	username, err := getUsernameFromContext(ctx)

	if err != nil {
		return &proto.RejectEventResponse{}, err
	}

	path := filepath.Join("EventConfirmations", strconv.FormatInt(request.GetEventId(), 10))

	confirmations := &proto.Confirmations{}
	confirmations, err = persistency.Load(node, path, confirmations)

	if err != nil {
		return &proto.RejectEventResponse{}, err
	}

	if _, ok := confirmations.Confirmations[username]; !ok {
		return &proto.RejectEventResponse{}, status.Error(codes.PermissionDenied, "")
	}

	err = persistency.Delete(node, path)

	if err != nil {
		return &proto.RejectEventResponse{}, err
	}

	path = filepath.Join("Event", strconv.FormatInt(request.GetEventId(), 10))

	event := &proto.Event{}
	event, err = persistency.Load(node, path, event)

	if err != nil {
		return &proto.RejectEventResponse{}, err
	}

	updateEventHistory(ctx, proto.Action_REJECT, event, []string{username})

	users := make([]string, 0)

	for key := range confirmations.Confirmations {
		users = append(users, key)
	}

	updateEventHistory(ctx, proto.Action_DELETE, event, users)

	err = persistency.Delete(node, path)

	if err != nil {
		return &proto.RejectEventResponse{}, err
	}

	path = filepath.Join("EventParticipants", strconv.FormatInt(request.GetEventId(), 10))

	err = persistency.Delete(node, path)

	if err != nil {
		return &proto.RejectEventResponse{}, err
	}

	return &proto.RejectEventResponse{}, nil
}

func StartEventService(network string, address string) {
	log.Infof("Event Service Started")

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

	proto.RegisterEventsServiceServer(s, &EventsServer{})

	if err := s.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}
