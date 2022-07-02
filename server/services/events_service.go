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

	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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

func (*EventsServer) CreateEvent(ctx context.Context, request *proto.CreateEventRequest) (*proto.CreateEventResponse, error) {
	log.Debugf("Create event invoked with %v\n", request)

	event := request.GetEvent()

	seed := rand.NewSource(time.Now().UnixNano())
	generator := rand.New(seed)
	event.Id = generator.Int63()

	users := make(map[string]void)

	for _, member := range request.GetUsers() {
		if _, ok := users[member]; ok {
			continue
		}
		users[member] = empty
	}

	keys := make([]string, 0, len(users))
	for k := range users {
		keys = append(keys, k)
	}

	err := checkValid(event, keys)

	if err != nil {
		return &proto.CreateEventResponse{Result: proto.OperationOutcome_FAILED}, err
	}

	err = persistency.Save(event, filepath.Join("Event", strconv.FormatInt(event.Id, 10)))

	if err != nil {
		return &proto.CreateEventResponse{Result: proto.OperationOutcome_FAILED}, err
	}

	err = persistency.Save(users, filepath.Join("EventParticipants", strconv.FormatInt(event.Id, 10)))
	if err != nil {
		return &proto.CreateEventResponse{Result: proto.OperationOutcome_FAILED}, err
	}

	updateEventHistory(ctx, proto.Action_CREATE, event, keys)

	return &proto.CreateEventResponse{Result: proto.OperationOutcome_SUCCESS}, nil
}

func (*EventsServer) DeleteEvent(ctx context.Context, request *proto.DeleteEventRequest) (*proto.DeleteEventResponse, error) {
	log.Debugf("Delete Event invoked with %v\n", request)

	id := request.GetId()

	path := filepath.Join("Event", strconv.FormatInt(id, 10))

	event, err := persistency.Load[proto.Event](path)

	if err != nil {
		return &proto.DeleteEventResponse{Result: proto.OperationOutcome_FAILED}, err
	}

	err = persistency.Delete(path)

	if err != nil {
		return &proto.DeleteEventResponse{Result: proto.OperationOutcome_FAILED}, err
	}

	ppath := filepath.Join("EventParticipants", strconv.FormatInt(event.Id, 10))

	members, err := persistency.Load[[]string](ppath)

	if err != nil {
		return &proto.DeleteEventResponse{Result: proto.OperationOutcome_FAILED}, err
	}

	err = persistency.Delete(ppath)

	if err != nil {
		return &proto.DeleteEventResponse{Result: proto.OperationOutcome_FAILED}, err
	}

	err = updateEventHistory(ctx, proto.Action_DELETE, &event, members)

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

func updateEventHistory(ctx context.Context, action proto.Action, event *proto.Event, users []string) error {

	history := &HistoryServer{}

	_, err := history.AddHistoryEntry(ctx, &proto.AddHistoryEntryRequest{
		Entry: &proto.HistoryEntry{
			Action: action,
			Event:  event,
		},
		Users: users,
	})
	return err
}

func checkValid(event *proto.Event, users []string) error {
	for _, user := range users {
		events, err := getUserEvents(user)

		if err != nil {
			return err
		}

		for _, eve := range events {
			if eve.Start.Seconds < event.Start.Seconds && event.Start.Seconds < eve.End.Seconds {
				return status.Errorf(codes.Unavailable, "User %s already has plans", user)
			}
			if eve.Start.Seconds < event.End.Seconds && event.End.Seconds < eve.End.Seconds {
				return status.Errorf(codes.Unavailable, "User %s already has plans", user)
			}
			if event.Start.Seconds < eve.Start.Seconds && eve.Start.Seconds < event.End.Seconds {
				return status.Errorf(codes.Unavailable, "User %s already has plans", user)
			}
		}
	}
	return nil
}

func getUserEvents(username string) ([]proto.Event, error) {

	answer := []proto.Event{}
	events := make(map[int64]proto.Event)

	entries, err := persistency.Load[[]proto.HistoryEntry](filepath.Join("History", username))

	if err != nil {
		return nil, err
	}

	for _, entry := range entries {
		if entry.Event != nil {
			if entry.Action == proto.Action_DELETE {
				delete(events, entry.Event.Id)
			} else if entry.Action == proto.Action_CREATE {
				events[entry.Event.Id] = *entry.Event
			}
		}
	}

	for k := range events {
		answer = append(answer, events[k])
	}

	return answer, nil
}
