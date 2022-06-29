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
	"time"

	"github.com/dgrijalva/jwt-go"
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
	user, err := persistency.Load[proto.User](filepath.Join("User", request.GetUsername()))
	if err != nil {
		return nil, err
	}
	err = bcrypt.CompareHashAndPassword([]byte(user.PasswordHash), []byte(request.Password))
	if err != nil {
		log.Infof("Permission denied:\n%v\n", err)
		return nil, status.Errorf(codes.PermissionDenied, "")
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
		log.Errorf("Error creating token:\n%v\n", err)
		return nil, status.Errorf(codes.Internal, err.Error())
	}

	return &proto.LoginResponse{Token: tokenString}, nil
}

func StartAuthServer(rsaPrivateKey string, network string, address string) {
	log.Infof("Auth service started\n")

	lis, err := net.Listen(network, address)

	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	key, err := ioutil.ReadFile(rsaPrivateKey)
	if err != nil {
		log.Fatalf("Error reading the jwt private key:\n%v\n", err)
	}
	parsedKey, err := jwt.ParseRSAPrivateKeyFromPEM(key)
	if err != nil {
		log.Fatalf("Error parsing the jwt private key:\n%v\n", err)
	}
	s := grpc.NewServer()
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

	key, err := ioutil.ReadFile("pub.pem")

	if err != nil {
		log.Fatalf("Error reading the jwt public key:\n%v\n", err)
	}

	publicKey, err := jwt.ParseRSAPublicKeyFromPEM(key)

	if err != nil {
		log.Fatalf("Error parsing the jwt public key:\n%v\n", err)
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
