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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type void struct{}

var member void

type GroupsServer struct {
	proto.UnimplementedGroupServiceServer
}

func (*GroupsServer) CreateGroup(_ context.Context, request *proto.CreateGroupRequest) (*proto.CreateGroupResponse, error) {
	log.Debugf("Create group invoked with %v\n", request)

	group := request.GetGroup()
	err := persistency.Save(group, filepath.Join("Group", strconv.FormatInt(group.Id, 10)))

	if err != nil {
		return &proto.CreateGroupResponse{Result: proto.OperationOutcome_FAILED}, err
	}

	members := make(map[proto.UserLevel]map[int64]void)

	err = persistency.Save(members, filepath.Join("GroupMembers", strconv.FormatInt(group.Id, 10)))
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

func (*GroupsServer) EditGroup(_ context.Context, request *proto.EditGroupRequest) (*proto.EditGroupResponse, error) {
	log.Debugf("Edit Group invoked with %v\n", request)

	group := request.GetGroup()
	err := persistency.Save(group, filepath.Join("Group", strconv.FormatInt(group.Id, 10)))

	if err != nil {
		return &proto.EditGroupResponse{Result: proto.OperationOutcome_FAILED}, err
	}

	return &proto.EditGroupResponse{Result: proto.OperationOutcome_SUCCESS}, nil
}

func (*GroupsServer) DeleteGroup(_ context.Context, request *proto.DeleteGroupRequest) (*proto.DeleteGroupResponse, error) {
	log.Debugf("Delete Group invoked with %v\n", request)

	id := request.GetId()
	err := persistency.Delete(filepath.Join("Group", strconv.FormatInt(id, 10)))

	if err != nil {
		return &proto.DeleteGroupResponse{Result: proto.OperationOutcome_FAILED}, err
	}

	err = persistency.Delete(filepath.Join("GroupMembers", strconv.FormatInt(id, 10)))
	if err != nil {
		return &proto.DeleteGroupResponse{Result: proto.OperationOutcome_FAILED}, err
	}

	return &proto.DeleteGroupResponse{Result: proto.OperationOutcome_SUCCESS}, nil
}

func (*GroupsServer) AddUser(_ context.Context, request *proto.AddUserRequest) (*proto.AddUserResponse, error) {
	log.Debugf("Add User invoked with %v\n", request)

	userID := request.GetUserID()
	groupID := request.GetGroupID()
	level := request.GetLevel()

	path := filepath.Join("GroupMembers", strconv.FormatInt(groupID, 10))

	groupMembers, err := persistency.Load[map[proto.UserLevel]map[int64]void](path)

	if err != nil {
		return &proto.AddUserResponse{Result: proto.OperationOutcome_FAILED}, err
	}

	groupMembers[level][userID] = member

	err = persistency.Save(groupMembers, filepath.Join(path))

	if err != nil {
		return &proto.AddUserResponse{Result: proto.OperationOutcome_FAILED}, err
	}

	return &proto.AddUserResponse{Result: proto.OperationOutcome_SUCCESS}, nil
}

func (*GroupsServer) GetGroupUsers(request *proto.GetGroupUsersRequest, server proto.GroupService_GetGroupUsersServer) error {
	log.Debugf("Get Group Users invoked with %v\n", request)

	groupID := request.GetGroupID()
	level := request.GetLevel()

	path := filepath.Join("GroupMembers", strconv.FormatInt(groupID, 10))

	groupMembers, err := persistency.Load[map[proto.UserLevel]map[int64]void](path)

	if err != nil {
		return err
	}

	for key := range groupMembers[level] {
		user, err := persistency.Load[proto.User](filepath.Join("User", strconv.FormatInt(key, 10)))

		if err != nil {
			return err
		}

		err = server.Send(&proto.GetGroupUsersResponse{User: &user})

		if err != nil {
			log.Errorf("Error sending response GroupUser:\n%v\n", err)
			return status.Error(codes.Internal, "Error sending response")
		}
	}

	return nil
}

func (*GroupsServer) RemoveUser(_ context.Context, request *proto.RemoveUserRequest) (*proto.RemoveUserResponse, error) {
	log.Debugf("Remove User invoked with %v\n", request)

	userID := request.GetUserID()
	groupID := request.GetGroupID()
	level := request.GetLevel()

	path := filepath.Join("GroupMembers", strconv.FormatInt(groupID, 10))

	groupMembers, err := persistency.Load[map[proto.UserLevel]map[int64]void](path)

	if err != nil {
		return &proto.RemoveUserResponse{Result: proto.OperationOutcome_FAILED}, err
	}

	delete(groupMembers[level], userID)

	err = persistency.Save(groupMembers, filepath.Join(path))

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
