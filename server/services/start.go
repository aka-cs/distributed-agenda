package services

import (
	"server/chord"

	log "github.com/sirupsen/logrus"
)

var (
	node *chord.Node
)

func Start(rsaPrivateKeyPath string, rsaPublicKeyPath string, network string) {
	var err error
	node, err = chord.DefaultNode("50050")

	if err != nil {
		log.Fatalf("Can't start chord node")
	}

	err = node.Start()

	if err != nil {
		log.Fatalf("Error starting node")
	}

	go StartGroupService(network, "0.0.0.0:50052")
	go StartEventService(network, "0.0.0.0:50053")
	go StartAuthServer(rsaPrivateKeyPath, network, "0.0.0.0:50054")
	go StartHistoryService(network, "0.0.0.0:50055")
	go StartUserService(network, "0.0.0.0:50051")
}
