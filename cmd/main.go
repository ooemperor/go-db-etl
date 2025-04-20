package main

import (
	"go-db-etl/pkg/config"
	"go-db-etl/pkg/logging"
)

func main() {
	logging.EtlLogger.Info("Starting " + config.Config.Name)

}
