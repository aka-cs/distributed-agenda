package main

import (
	"server/logging"
	"server/services"
)

func main() {

	logging.SettingLogger("server")

	rsaKeyPath := "pv.pem"
	network := "tcp"

	go services.StartGroupService(network, "0.0.0.0:50052")
	go services.StartEventService(network, "0.0.0.0:50053")
	go services.StartAuthServer(rsaKeyPath, network, "0.0.0.0:50054")
	go services.StartHistoryService(network, "0.0.0.0:50055")
	services.StartUserService(network, "0.0.0.0:50051")
}
