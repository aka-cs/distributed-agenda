package services

import (
	"context"
	"path/filepath"
	"server/persistency"
	"server/proto"
	"strconv"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func updateGroupHistory(ctx context.Context, action proto.Action, group *proto.Group, users []string) error {

	history := &HistoryServer{}

	_, err := history.AddHistoryEntry(ctx, &proto.AddHistoryEntryRequest{
		Entry: &proto.HistoryEntry{
			Action: action,
			Target: &proto.HistoryEntry_Group{Group: group},
		},
		Users: users,
	})
	return err
}

func getGroupUsernames(group *proto.Group) ([]string, error) {
	path := filepath.Join("GroupMembers", strconv.FormatInt(group.Id, 10))

	groupMembers := &proto.GroupMembers{}
	groupMembers, err := persistency.Load(node, path, groupMembers)

	if err != nil {
		return nil, err
	}

	usernames := []string{}

	for level := range groupMembers.Members {
		for _, username := range groupMembers.Members[level].Users {
			usernames = append(usernames, username)
		}
	}

	return usernames, nil
}

func checkIsGroupOwner(username string, groupId int64) (bool, error) {

	path := filepath.Join("History", username)

	history := &proto.History{}
	history, err := persistency.Load(node, path, history)
	if err != nil {
		return false, err
	}

	count := 0

	for i := 0; i < len(history.Entries); i++ {
		switch x := history.Entries[i].Target.(type) {
		case *proto.HistoryEntry_Group:
			if x.Group.Id == groupId {
				if history.Entries[i].Action == proto.Action_CREATE {
					count++
				} else if history.Entries[i].Action == proto.Action_DELETE {
					count--
				}
			}
		}
	}
	return count != 0, nil
}

func getUsernameFromContext(ctx context.Context) (string, error) {
	md, ok := metadata.FromIncomingContext(ctx)

	if !ok {
		return "", status.Error(codes.Internal, "")
	}

	return md["username"][0], nil
}

func hasHierarchy(group *proto.Group) (bool, error) {
	path := filepath.Join("GroupMembers", strconv.FormatInt(group.Id, 10))

	groupMembers := &proto.GroupMembers{}
	groupMembers, err := persistency.Load(node, path, groupMembers)

	if err != nil {
		return false, err
	}

	return len(groupMembers.Members[int32(proto.UserLevel_ADMIN)].GetUsers()) != 0, nil
}

func updateEventHistory(ctx context.Context, action proto.Action, event *proto.Event, users []string) error {

	history := &HistoryServer{}

	_, err := history.AddHistoryEntry(ctx, &proto.AddHistoryEntryRequest{
		Entry: &proto.HistoryEntry{
			Action: action,
			Target: &proto.HistoryEntry_Event{Event: event},
		},
		Users: users,
	})
	return err
}

func checkValid(event *proto.Event, users []string) ([]string, error) {

	invalid := make([]string, 0)

	for _, user := range users {
		events, err := getUserEvents(user)

		if err != nil {
			return nil, err
		}

		for _, eve := range events {
			if eve.Start.Seconds < event.Start.Seconds && event.Start.Seconds < eve.End.Seconds {
				invalid = append(invalid, user)
			} else if eve.Start.Seconds < event.End.Seconds && event.End.Seconds < eve.End.Seconds {
				invalid = append(invalid, user)
			} else if event.Start.Seconds < eve.Start.Seconds && eve.Start.Seconds < event.End.Seconds {
				invalid = append(invalid, user)
			}
		}
	}

	if len(invalid) != 0 {
		return invalid, status.Error(codes.Unavailable, "Some users already have plans")
	}

	return nil, nil
}

func getUserEvents(username string) ([]proto.Event, error) {

	answer := []proto.Event{}
	events := make(map[int64]proto.Event)

	history := &proto.History{}
	history, err := persistency.Load(node, filepath.Join("History", username), history)

	if err != nil {
		return nil, err
	}

	entries := history.Entries

	for _, entry := range entries {
		switch x := entry.Target.(type) {
		case *proto.HistoryEntry_Event:
			if entry.Action == proto.Action_DELETE {
				delete(events, x.Event.Id)
			} else if entry.Action == proto.Action_CREATE {
				events[x.Event.Id] = *x.Event
			}
		}
	}

	for k := range events {
		answer = append(answer, events[k])
	}

	return answer, nil
}

func remove[T any](s []T, i int) []T {
	s[i] = s[len(s)-1]
	return s[:len(s)-1]
}
