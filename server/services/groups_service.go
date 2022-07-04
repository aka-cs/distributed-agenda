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

type void struct{}

var empty void

type GroupsServer struct {
	proto.UnimplementedGroupServiceServer
}

func (*GroupsServer) CreateGroup(ctx context.Context, request *proto.CreateGroupRequest) (*proto.CreateGroupResponse, error) {

	username, err := getUsernameFromContext(ctx)

	if err != nil {
		return &proto.CreateGroupResponse{}, err
	}

	all_users := make(map[string]void)

	all_users[username] = empty

	group := request.GetGroup()

	seed := rand.NewSource(time.Now().UnixNano())
	generator := rand.New(seed)
	group.Id = generator.Int63()

	err = persistency.Save(node, group, filepath.Join("Group", strconv.FormatInt(group.Id, 10)))

	if err != nil {
		return &proto.CreateGroupResponse{}, err
	}

	members := proto.GroupMembers{Members: make(map[int32]*proto.UserList)}
	members.Members[int32(proto.UserLevel_ADMIN)] = &proto.UserList{}
	members.Members[int32(proto.UserLevel_USER)] = &proto.UserList{}

	if request.GetHierarchy() {
		members.Members[int32(proto.UserLevel_ADMIN)].Users = append(members.Members[int32(proto.UserLevel_ADMIN)].Users, username)
	} else {
		members.Members[int32(proto.UserLevel_USER)].Users = append(members.Members[int32(proto.UserLevel_USER)].Users, username)
	}

	for _, member := range request.GetUsers() {
		if _, ok := all_users[member]; ok {
			continue
		}
		if !isUser(username) {
			return &proto.CreateGroupResponse{}, status.Errorf(codes.NotFound, "User %s not found", member)
		}
		members.Members[int32(proto.UserLevel_USER)].Users = append(members.Members[int32(proto.UserLevel_USER)].Users, member)
		all_users[member] = empty
	}

	if !request.GetHierarchy() {
		for _, member := range request.GetAdmins() {
			if _, ok := all_users[member]; ok {
				continue
			}
			members.Members[int32(proto.UserLevel_ADMIN)].Users = append(members.Members[int32(proto.UserLevel_ADMIN)].Users, member)
			all_users[member] = empty
		}
	}

	err = persistency.Save(node, &members, filepath.Join("GroupMembers", strconv.FormatInt(group.Id, 10)))
	if err != nil {
		return &proto.CreateGroupResponse{}, err
	}

	if request.GetHierarchy() {
		delete(all_users, username)
		err = updateGroupHistory(ctx, proto.Action_CREATE, group, []string{username})

		if err != nil {
			return &proto.CreateGroupResponse{}, err
		}
	}

	keys := make([]string, 0, len(all_users))
	for k := range all_users {
		keys = append(keys, k)
	}

	err = updateGroupHistory(ctx, proto.Action_JOINED, group, keys)

	if err != nil {
		return &proto.CreateGroupResponse{}, err
	}

	return &proto.CreateGroupResponse{}, nil
}

func (*GroupsServer) GetGroup(_ context.Context, request *proto.GetGroupRequest) (*proto.GetGroupResponse, error) {

	id := request.GetId()
	group := &proto.Group{}
	group, err := persistency.Load(node, filepath.Join("Group", strconv.FormatInt(id, 10)), group)

	if err != nil {
		return nil, err
	}

	return &proto.GetGroupResponse{Group: group}, nil
}

func (*GroupsServer) EditGroup(ctx context.Context, request *proto.EditGroupRequest) (*proto.EditGroupResponse, error) {

	group := request.GetGroup()
	err := persistency.Save(node, group, filepath.Join("Group", strconv.FormatInt(group.Id, 10)))

	if err != nil {
		return &proto.EditGroupResponse{}, err
	}

	usernames, err := getGroupUsernames(group)

	if err != nil {
		return &proto.EditGroupResponse{}, err
	}

	updateGroupHistory(ctx, proto.Action_UPDATE, group, usernames)

	return &proto.EditGroupResponse{}, nil
}

func (*GroupsServer) DeleteGroup(ctx context.Context, request *proto.DeleteGroupRequest) (*proto.DeleteGroupResponse, error) {

	id := request.GetId()

	group := &proto.Group{}
	group, err := persistency.Load(node, filepath.Join("Group", strconv.FormatInt(id, 10)), group)

	if err != nil {
		return &proto.DeleteGroupResponse{}, err
	}

	err = persistency.Delete(node, filepath.Join("Group", strconv.FormatInt(id, 10)))

	if err != nil {
		return &proto.DeleteGroupResponse{}, err
	}

	err = persistency.Delete(node, filepath.Join("GroupMembers", strconv.FormatInt(id, 10)))
	if err != nil {
		return &proto.DeleteGroupResponse{}, err
	}

	usernames, err := getGroupUsernames(group)

	if err != nil {
		return &proto.DeleteGroupResponse{}, err
	}

	updateGroupHistory(ctx, proto.Action_DELETE, group, usernames)

	return &proto.DeleteGroupResponse{}, nil
}

func (*GroupsServer) AddUser(ctx context.Context, request *proto.AddUserRequest) (*proto.AddUserResponse, error) {

	userID := request.GetUserID()
	groupID := request.GetGroupID()
	level := request.GetLevel()

	path := filepath.Join("GroupMembers", strconv.FormatInt(groupID, 10))

	groupMembers := &proto.GroupMembers{}
	groupMembers, err := persistency.Load(node, path, groupMembers)

	if err != nil {
		return &proto.AddUserResponse{}, err
	}

	username, err := getUsernameFromContext(ctx)

	if err != nil {
		return &proto.AddUserResponse{}, err
	}

	if !isUser(userID) {
		return &proto.AddUserResponse{}, status.Errorf(codes.NotFound, "User %s not found", username)
	}

	isOwner, err := checkIsGroupOwner(username, groupID)

	if err != nil {
		return &proto.AddUserResponse{}, err
	}

	if level == proto.UserLevel_ADMIN && !isOwner {
		return &proto.AddUserResponse{}, status.Error(codes.PermissionDenied, "Only creator can add admins")
	}

	if level == proto.UserLevel_USER && len(groupMembers.Members[int32(proto.UserLevel_ADMIN)].Users) != 0 {
		admins := groupMembers.Members[int32(proto.UserLevel_ADMIN)].Users
		ok := false
		for i := range admins {
			if admins[i] == username {
				ok = true
				break
			}
		}
		if !ok {
			return &proto.AddUserResponse{}, status.Error(codes.PermissionDenied, "Only admins can add users")
		}
	}

	users := groupMembers.Members[int32(level)].Users

	ok := false
	for i := range users {
		if users[i] == username {
			ok = true
			break
		}
	}
	if !ok {
		return &proto.AddUserResponse{}, status.Error(codes.AlreadyExists, "User is already in group")
	}

	groupMembers.Members[int32(level)].Users = append(groupMembers.Members[int32(level)].Users, userID)

	err = persistency.Save(node, groupMembers, filepath.Join(path))

	if err != nil {
		return &proto.AddUserResponse{}, err
	}

	group := &proto.Group{}
	group, err = persistency.Load(node, filepath.Join("Group", strconv.FormatInt(groupID, 10)), group)

	if err != nil {
		return &proto.AddUserResponse{}, err
	}

	err = updateGroupHistory(ctx, proto.Action_JOINED, group, []string{userID})

	if err != nil {
		return &proto.AddUserResponse{}, err
	}

	return &proto.AddUserResponse{}, nil
}

func (*GroupsServer) GetGroupUsers(request *proto.GetGroupUsersRequest, server proto.GroupService_GetGroupUsersServer) error {
	groupID := request.GetGroupID()

	path := filepath.Join("GroupMembers", strconv.FormatInt(groupID, 10))

	groupMembers := &proto.GroupMembers{}
	groupMembers, err := persistency.Load(node, path, groupMembers)

	if err != nil {
		return err
	}

	for level := range groupMembers.Members {
		for key := range groupMembers.Members[level].Users {
			user := &proto.User{}
			user, err := persistency.Load(node, filepath.Join("User", groupMembers.Members[level].Users[key]), user)

			if err != nil {
				return err
			}

			user.PasswordHash = ""

			err = server.Send(&proto.GetGroupUsersResponse{User: user, Level: proto.UserLevel(level)})

			if err != nil {
				log.Errorf("Error sending response GroupUser: %v", err)
				return status.Error(codes.Internal, "Error sending response")
			}
		}
	}
	return nil
}

func (*GroupsServer) RemoveUser(ctx context.Context, request *proto.RemoveUserRequest) (*proto.RemoveUserResponse, error) {
	username, err := getUsernameFromContext(ctx)

	if err != nil {
		return &proto.RemoveUserResponse{}, err
	}

	userID := request.GetUserID()
	groupID := request.GetGroupID()
	level := request.GetLevel()

	isOwner, err := checkIsGroupOwner(username, groupID)

	if err != nil {
		return &proto.RemoveUserResponse{}, err
	}

	if !isOwner {
		return &proto.RemoveUserResponse{}, status.Error(codes.PermissionDenied, "Only creator can remove users")
	}

	path := filepath.Join("GroupMembers", strconv.FormatInt(groupID, 10))

	groupMembers := &proto.GroupMembers{}
	groupMembers, err = persistency.Load(node, path, groupMembers)

	if err != nil {
		return &proto.RemoveUserResponse{}, err
	}

	users := groupMembers.Members[int32(level)].GetUsers()
	i := 0
	for i = range users {
		if users[i] == userID {
			break
		}
	}

	groupMembers.Members[int32(level)].Users = remove(groupMembers.Members[int32(level)].Users, i)

	err = persistency.Save(node, groupMembers, filepath.Join(path))

	if err != nil {
		return &proto.RemoveUserResponse{}, err
	}

	group := &proto.Group{}
	group, err = persistency.Load(node, filepath.Join("Group", strconv.FormatInt(groupID, 10)), group)

	if err != nil {
		return &proto.RemoveUserResponse{}, err
	}

	err = updateGroupHistory(ctx, proto.Action_LEFT, group, []string{userID})

	if err != nil {
		return &proto.RemoveUserResponse{}, err
	}

	return &proto.RemoveUserResponse{}, nil
}

func StartGroupService(network string, address string) {
	log.Infof("Group Service Started")

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

	proto.RegisterGroupServiceServer(s, &GroupsServer{})

	if err := s.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}
