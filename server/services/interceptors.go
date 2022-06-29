package services

import (
	"context"
	"time"

	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

func UnaryServerInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {

	log.Debugf("Request received - Method:%s\n", info.FullMethod)

	start := time.Now()

	_, err := ValidateRequest(ctx)

	if err != nil {
		return nil, err
	}

	// Calls the handler
	h, err := handler(ctx, req)

	// Logging with grpclog (grpclog.LoggerV2)
	log.Debugf("Request completed - Method:%s\tDuration:%s\tError:%v\n",
		info.FullMethod,
		time.Since(start),
		err)

	return h, err
}

func StreamServerInterceptor(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {

	log.Debugf("Streaming Request received - Method:%s\n", info.FullMethod)

	ctx := ss.Context()

	start := time.Now()

	_, err := ValidateRequest(ctx)

	if err != nil {
		return err
	}

	// Calls the handler
	err = handler(srv, ss)

	// Logging with grpclog (grpclog.LoggerV2)
	log.Debugf("Streaming Request completed - Method:%s\tDuration:%s\tError:%v\n",
		info.FullMethod,
		time.Since(start),
		err)

	return err
}
