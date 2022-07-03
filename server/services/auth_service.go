package services

import (
	"context"
	"crypto/rsa"
	"errors"
	"io/ioutil"
	"net"
	"path/filepath"
	"server/persistency"
	"server/proto"
	"strings"
	"time"

	"github.com/dgrijalva/jwt-go"
	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	log "github.com/sirupsen/logrus"
	"golang.org/x/crypto/bcrypt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type AuthServer struct {
	*proto.UnimplementedAuthServer
	jwtPrivateKey *rsa.PrivateKey
}

func (server *AuthServer) Login(_ context.Context, request *proto.LoginRequest) (*proto.LoginResponse, error) {

	user, err := persistency.Load[proto.User](node, filepath.Join("User", request.GetUsername()))
	if err != nil {
		return nil, err
	}
	err = bcrypt.CompareHashAndPassword([]byte(user.PasswordHash), []byte(request.Password))
	if err != nil {
		log.Infof("Permission denied: %v", err)
		return nil, status.Errorf(codes.PermissionDenied, "Wrong username or password")
	}

	claims := make(jwt.MapClaims)
	claims["exp"] = time.Now().Add(time.Hour * 72).Unix()
	claims["iss"] = "auth.service"
	claims["iat"] = time.Now().Unix()
	claims["email"] = user.Email
	claims["sub"] = user.Username
	claims["name"] = user.Name

	token := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)

	tokenString, err := token.SignedString(server.jwtPrivateKey)
	if err != nil {
		log.Errorf("Error creating token: %v", err)
		return nil, status.Errorf(codes.Internal, "")
	}

	return &proto.LoginResponse{Token: tokenString}, nil
}

func (*AuthServer) SignUp(_ context.Context, request *proto.SignUpRequest) (*proto.SignUpResponse, error) {

	user := request.GetUser()
	user.Username = strings.ToLower(user.Username)
	path := filepath.Join("User", user.Username)

	if persistency.FileExists(node, path) {
		return &proto.SignUpResponse{}, status.Error(codes.AlreadyExists, "Username is taken")
	}

	err := persistency.Save(node, user, path)

	if err != nil {
		return &proto.SignUpResponse{}, err
	}

	path = filepath.Join("History", user.Username)

	err = persistency.Save(node, []proto.HistoryEntry{}, path)

	if err != nil {
		return &proto.SignUpResponse{}, err
	}

	return &proto.SignUpResponse{}, nil
}

func StartAuthServer(network string, address string) {
	log.Infof("Auth service started")

	lis, err := net.Listen(network, address)

	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	key, err := ioutil.ReadFile(rsaPrivate)
	if err != nil {
		log.Fatalf("Error reading the jwt private key: %v", err)
	}
	parsedKey, err := jwt.ParseRSAPrivateKeyFromPEM(key)
	if err != nil {
		log.Fatalf("Error parsing the jwt private key: %v", err)
	}

	s := grpc.NewServer(
		grpc.UnaryInterceptor(
			grpc_middleware.ChainUnaryServer(
				UnaryLoggingInterceptor,
			),
		), grpc.StreamInterceptor(
			grpc_middleware.ChainStreamServer(
				StreamLoggingInterceptor,
			),
		),
	)

	proto.RegisterAuthServer(s, &AuthServer{jwtPrivateKey: parsedKey})

	if err := s.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}

func validateToken(token string, publicKey *rsa.PublicKey) (*jwt.Token, error) {
	jwtToken, err := jwt.Parse(token, func(t *jwt.Token) (interface{}, error) {
		if _, ok := t.Method.(*jwt.SigningMethodRSA); !ok {
			log.Errorf("Unexpected signing method: %v", t.Header["alg"])
			return nil, errors.New("invalid token")
		}
		return publicKey, nil
	})
	if err == nil && jwtToken.Valid {
		return jwtToken, nil
	}
	return nil, err
}

func ValidateRequest(ctx context.Context) (*jwt.Token, error) {
	var (
		token *jwt.Token
		err   error
	)

	key, err := ioutil.ReadFile(rsaPublic)

	if err != nil {
		log.Fatalf("Error reading the jwt public key: %v", err)
	}

	publicKey, err := jwt.ParseRSAPublicKeyFromPEM(key)

	if err != nil {
		log.Fatalf("Error parsing the jwt public key: %v", err)
	}

	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, status.Errorf(codes.Unauthenticated, "valid token required.")
	}

	jwtToken, ok := md["authorization"]

	if !ok {
		return nil, status.Errorf(codes.Unauthenticated, "valid token required.")
	}

	token, err = validateToken(jwtToken[0], publicKey)
	if err != nil {
		return nil, status.Errorf(codes.Unauthenticated, "valid token required.")
	}

	return token, nil
}
