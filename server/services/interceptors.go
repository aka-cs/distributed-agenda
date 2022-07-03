package services

import (
	"context"
	"time"

	"github.com/dgrijalva/jwt-go"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

func UnaryServerInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {

	token, err := ValidateRequest(ctx)

	if err != nil {
		return nil, err
	}

	md, ok := metadata.FromIncomingContext(ctx)

	if !ok {
		log.Error("Error extracting metadata from context")
		return nil, status.Error(codes.Internal, "")
	}

	md.Append("username", token.Claims.(jwt.MapClaims)["sub"].(string))

	newCtx := metadata.NewIncomingContext(ctx, md)

	h, err := handler(newCtx, req)

	return h, err
}

func StreamServerInterceptor(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {

	ctx := ss.Context()

	_, err := ValidateRequest(ctx)

	if err != nil {
		return err
	}

	err = handler(srv, ss)

	return err
}

func UnaryLoggingInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	p, _ := peer.FromContext(ctx)

	log.Debugf("Request received - Method:%s From:%s", info.FullMethod, p.Addr.String())

	start := time.Now()

	h, err := handler(ctx, req)

	log.Debugf("Request completed - Method:%s\tDuration:%s\tError:%v",
		info.FullMethod,
		time.Since(start),
		err)

	return h, err
}

func StreamLoggingInterceptor(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {

	ctx := ss.Context()

	p, _ := peer.FromContext(ctx)

	log.Debugf("Streaming request received - Method:%s From:%s", info.FullMethod, p.Addr.String())

	start := time.Now()

	err := handler(srv, ss)

	log.Debugf("Streaming Request completed - Method:%s\tDuration:%s\tError:%v",
		info.FullMethod,
		time.Since(start),
		err)

	return err
}
