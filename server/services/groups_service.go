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

	"github.com/dgrijalva/jwt-go"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type void struct{}

var empty void

type GroupsServer struct {
	proto.UnimplementedGroupServiceServer
}

func (*GroupsServer) CreateGroup(ctx context.Context, request *proto.CreateGroupRequest) (*proto.CreateGroupResponse, error) {
	log.Debugf("Create group invoked with %v\n", request)

	username, err := getUsernameFromContext(ctx)

	if err != nil {
		return &proto.CreateGroupResponse{Result: proto.OperationOutcome_FAILED}, status.Error(codes.Internal, "")
	}

	all_users := make(map[string]void)

	all_users[username] = empty

	group := request.GetGroup()

	seed := rand.NewSource(time.Now().UnixNano())
	generator := rand.New(seed)
	group.Id = generator.Int63()

	err = persistency.Save(group, filepath.Join("Group", strconv.FormatInt(group.Id, 10)))

	if err != nil {
		return &proto.CreateGroupResponse{Result: proto.OperationOutcome_FAILED}, err
	}

	members := make(map[proto.UserLevel]map[string]void)
	members[proto.UserLevel_ADMIN] = make(map[string]void)
	members[proto.UserLevel_USER] = make(map[string]void)

	if request.GetHierarchy() {
		members[proto.UserLevel_ADMIN][username] = empty
	} else {
		members[proto.UserLevel_USER][username] = empty
	}

	for _, member := range request.GetUsers() {
		if _, ok := all_users[member]; ok {
			continue
		}
		members[proto.UserLevel_USER][member] = empty
		all_users[member] = empty
	}

	if !request.GetHierarchy() {
		for _, member := range request.GetAdmins() {
			if _, ok := all_users[member]; ok {
				continue
			}
			members[proto.UserLevel_ADMIN][member] = empty
			all_users[member] = empty
		}
	}

	err = persistency.Save(members, filepath.Join("GroupMembers", strconv.FormatInt(group.Id, 10)))
	if err != nil {
		return &proto.CreateGroupResponse{Result: proto.OperationOutcome_FAILED}, err
	}

	if request.GetHierarchy() {
		delete(all_users, username)
		err = updateGroupHistory(ctx, proto.Action_CREATE, group, []string{username})

		if err != nil {
			return &proto.CreateGroupResponse{Result: proto.OperationOutcome_FAILED}, err
		}
	}

	keys := make([]string, 0, len(all_users))
	for k := range all_users {
		keys = append(keys, k)
	}

	err = updateGroupHistory(ctx, proto.Action_JOINED, group, keys)

	if err != nil {
		return &proto.CreateGroupResponse{Result: proto.OperationOutcome_FAILED}, err
	}

	return &proto.CreateGroupResponse{Result: proto.OperationOutcome_SUCCESS}, nil
}

func (*GroupsServer) GetGroup(_ context.Context, request *proto.GetGroupRequest) (*proto.GetGroupResponse, error) {
	log.Debugf("Get group invoked with %v\n", request)

	id := request.GetId()
	group, err := persistency.Load[proto.Group](filepath.Join("Group", strconv.FormatInt(id, 10)))

	if err != nil {
		return nil, err
	}

	return &proto.GetGroupResponse{Group: &group}, nil
}

func (*GroupsServer) EditGroup(ctx context.Context, request *proto.EditGroupRequest) (*proto.EditGroupResponse, error) {
	log.Debugf("Edit Group invoked with %v\n", request)

	group := request.GetGroup()
	err := persistency.Save(group, filepath.Join("Group", strconv.FormatInt(group.Id, 10)))

	if err != nil {
		return &proto.EditGroupResponse{Result: proto.OperationOutcome_FAILED}, err
	}

	usernames, err := getGroupUsernames(group)

	if err != nil {
		return &proto.EditGroupResponse{Result: proto.OperationOutcome_FAILED}, err
	}

	updateGroupHistory(ctx, proto.Action_UPDATE, group, usernames)

	return &proto.EditGroupResponse{Result: proto.OperationOutcome_SUCCESS}, nil
}

func (*GroupsServer) DeleteGroup(ctx context.Context, request *proto.DeleteGroupRequest) (*proto.DeleteGroupResponse, error) {
	log.Debugf("Delete Group invoked with %v\n", request)

	id := request.GetId()

	group, err := persistency.Load[proto.Group](filepath.Join("Group", strconv.FormatInt(id, 10)))

	if err != nil {
		return &proto.DeleteGroupResponse{Result: proto.OperationOutcome_FAILED}, err
	}

	err = persistency.Delete(filepath.Join("Group", strconv.FormatInt(id, 10)))

	if err != nil {
		return &proto.DeleteGroupResponse{Result: proto.OperationOutcome_FAILED}, err
	}

	err = persistency.Delete(filepath.Join("GroupMembers", strconv.FormatInt(id, 10)))
	if err != nil {
		return &proto.DeleteGroupResponse{Result: proto.OperationOutcome_FAILED}, err
	}

	usernames, err := getGroupUsernames(&group)

	if err != nil {
		return &proto.DeleteGroupResponse{Result: proto.OperationOutcome_FAILED}, err
	}

	updateGroupHistory(ctx, proto.Action_DELETE, &group, usernames)

	return &proto.DeleteGroupResponse{Result: proto.OperationOutcome_SUCCESS}, nil
}

func (*GroupsServer) AddUser(ctx context.Context, request *proto.AddUserRequest) (*proto.AddUserResponse, error) {
	log.Debugf("Add User invoked with %v\n", request)

	username, err := getUsernameFromContext(ctx)

	if err != nil {
		return &proto.AddUserResponse{Result: proto.OperationOutcome_FAILED}, err
	}

	userID := request.GetUserID()
	groupID := request.GetGroupID()
	level := request.GetLevel()

	isOwner, err := checkIsOwner(username, groupID)

	if err != nil {
		return &proto.AddUserResponse{Result: proto.OperationOutcome_FAILED}, err
	}

	if !isOwner {
		return &proto.AddUserResponse{Result: proto.OperationOutcome_FAILED}, status.Error(codes.PermissionDenied, "Only creator can add users")
	}

	path := filepath.Join("GroupMembers", strconv.FormatInt(groupID, 10))

	groupMembers, err := persistency.Load[map[proto.UserLevel]map[string]void](path)

	if err != nil {
		return &proto.AddUserResponse{Result: proto.OperationOutcome_FAILED}, err
	}

	if _, ok := groupMembers[level][userID]; ok {
		return &proto.AddUserResponse{Result: proto.OperationOutcome_FAILED}, status.Error(codes.AlreadyExists, "User is already in group")
	}

	groupMembers[level][userID] = empty

	err = persistency.Save(groupMembers, filepath.Join(path))

	if err != nil {
		return &proto.AddUserResponse{Result: proto.OperationOutcome_FAILED}, err
	}

	group, err := persistency.Load[proto.Group](filepath.Join("Group", strconv.FormatInt(groupID, 10)))

	if err != nil {
		return &proto.AddUserResponse{Result: proto.OperationOutcome_FAILED}, err
	}

	err = updateGroupHistory(ctx, proto.Action_JOINED, &group, []string{userID})

	if err != nil {
		return &proto.AddUserResponse{Result: proto.OperationOutcome_FAILED}, err
	}

	return &proto.AddUserResponse{Result: proto.OperationOutcome_SUCCESS}, nil
}

func (*GroupsServer) GetGroupUsers(request *proto.GetGroupUsersRequest, server proto.GroupService_GetGroupUsersServer) error {
	log.Debugf("Get Group Users invoked with %v\n", request)

	groupID := request.GetGroupID()

	path := filepath.Join("GroupMembers", strconv.FormatInt(groupID, 10))

	groupMembers, err := persistency.Load[map[proto.UserLevel]map[string]void](path)

	if err != nil {
		return err
	}

	for level := range groupMembers {
		for key := range groupMembers[level] {
			user, err := persistency.Load[proto.User](filepath.Join("User", key))

			if err != nil {
				return err
			}

			user.PasswordHash = ""

			err = server.Send(&proto.GetGroupUsersResponse{User: &user, Level: level})

			if err != nil {
				log.Errorf("Error sending response GroupUser:\n%v\n", err)
				return status.Error(codes.Internal, "Error sending response")
			}
		}
	}
	return nil
}

func (*GroupsServer) RemoveUser(ctx context.Context, request *proto.RemoveUserRequest) (*proto.RemoveUserResponse, error) {
	log.Debugf("Remove User invoked with %v\n", request)

	username, err := getUsernameFromContext(ctx)

	if err != nil {
		return &proto.RemoveUserResponse{Result: proto.OperationOutcome_FAILED}, err
	}

	userID := request.GetUserID()
	groupID := request.GetGroupID()
	level := request.GetLevel()

	isOwner, err := checkIsOwner(username, groupID)

	if err != nil {
		return &proto.RemoveUserResponse{Result: proto.OperationOutcome_FAILED}, err
	}

	if !isOwner {
		return &proto.RemoveUserResponse{Result: proto.OperationOutcome_FAILED}, status.Error(codes.PermissionDenied, "Only creator can remove users")
	}

	path := filepath.Join("GroupMembers", strconv.FormatInt(groupID, 10))

	groupMembers, err := persistency.Load[map[proto.UserLevel]map[string]void](path)

	if err != nil {
		return &proto.RemoveUserResponse{Result: proto.OperationOutcome_FAILED}, err
	}

	delete(groupMembers[level], userID)

	err = persistency.Save(groupMembers, filepath.Join(path))

	if err != nil {
		return &proto.RemoveUserResponse{Result: proto.OperationOutcome_FAILED}, err
	}

	group, err := persistency.Load[proto.Group](filepath.Join("Group", strconv.FormatInt(groupID, 10)))

	if err != nil {
		return &proto.RemoveUserResponse{Result: proto.OperationOutcome_FAILED}, err
	}

	err = updateGroupHistory(ctx, proto.Action_LEFT, &group, []string{userID})

	if err != nil {
		return &proto.RemoveUserResponse{Result: proto.OperationOutcome_FAILED}, err
	}

	return &proto.RemoveUserResponse{Result: proto.OperationOutcome_SUCCESS}, nil
}

func StartGroupService(network string, address string) {
	log.Infof("Group Service Started\n")

	lis, err := net.Listen(network, address)

	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	s := grpc.NewServer(grpc.UnaryInterceptor(UnaryServerInterceptor), grpc.StreamInterceptor(StreamServerInterceptor))
	proto.RegisterGroupServiceServer(s, &GroupsServer{})

	if err := s.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}

func updateGroupHistory(ctx context.Context, action proto.Action, group *proto.Group, users []string) error {

	history := &HistoryServer{}

	_, err := history.AddHistoryEntry(ctx, &proto.AddHistoryEntryRequest{
		Entry: &proto.HistoryEntry{
			Action: action,
			Group:  group,
		},
		Users: users,
	})
	return err
}

func getGroupUsernames(group *proto.Group) ([]string, error) {
	path := filepath.Join("GroupMembers", strconv.FormatInt(group.Id, 10))

	groupMembers, err := persistency.Load[map[proto.UserLevel]map[string]void](path)

	if err != nil {
		return nil, err
	}

	usernames := []string{}

	for level := range groupMembers {
		for username := range groupMembers[level] {
			usernames = append(usernames, username)
		}
	}

	return usernames, nil
}

func checkIsOwner(username string, groupId int64) (bool, error) {

	path := filepath.Join("History", username)

	history, err := persistency.Load[[]proto.HistoryEntry](path)
	if err != nil {
		return false, err
	}

	count := 0

	for i := 0; i < len(history); i++ {
		if history[i].Group != nil && history[i].Group.Id == groupId {
			if history[i].Action == proto.Action_CREATE {
				count++
			} else if history[i].Action == proto.Action_DELETE {
				count--
			}
		}
	}
	return count != 0, nil
}

func getUsernameFromContext(ctx context.Context) (string, error) {
	jwtToken, err := ValidateRequest(ctx)

	if err != nil {
		return "", err
	}

	return jwtToken.Claims.(jwt.MapClaims)["sub"].(string), nil
}
