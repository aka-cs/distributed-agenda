package main

import (
	"server/logging"
	"server/services"
)

func main() {

	logging.SettingLogger("server")

	rsaPrivateKeyPath := "pv.pem"
	rsaPublicteKeyPath := "pub.pem"
	network := "tcp"

	services.Start(rsaPrivateKeyPath, rsaPublicteKeyPath, network)
}
