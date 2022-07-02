package services

import (
	"context"
	"time"

	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/peer"
)

func UnaryServerInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {

	_, err := ValidateRequest(ctx)

	if err != nil {
		return nil, err
	}

	h, err := handler(ctx, req)

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

	log.Debugf("Request received - Method:%s From:%s\n", info.FullMethod, p.Addr.String())

	start := time.Now()

	h, err := handler(ctx, req)

	log.Debugf("Request completed - Method:%s\tDuration:%s\tError:%v\n",
		info.FullMethod,
		time.Since(start),
		err)

	return h, err
}

func StreamLoggingInterceptor(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {

	ctx := ss.Context()

	p, _ := peer.FromContext(ctx)

	log.Debugf("Streaming request received - Method:%s From:%s\n", info.FullMethod, p.Addr.String())

	start := time.Now()

	err := handler(srv, ss)

	log.Debugf("Streaming Request completed - Method:%s\tDuration:%s\tError:%v\n",
		info.FullMethod,
		time.Since(start),
		err)

	return err
}
