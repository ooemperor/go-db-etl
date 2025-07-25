package main

import (
	"github.com/ooemperor/go-db-etl/pkg/config"
	"github.com/ooemperor/go-db-etl/pkg/logging"
	"github.com/ooemperor/go-db-etl/pkg/runner"
	"github.com/ooemperor/go-db-etl/pkg/sources"
)

func main() {
	logging.EtlLogger.Info("Starting " + config.Config.Name)

	logging.EtlLogger.Info("Building srcConfig")
	srcConfig, err := sources.BuildSourceConfig("./src.test.json")

	if err != nil {
		logging.EtlLogger.Error("Error building srcConfig: " + err.Error())
	}

	// TODO: INSERT pre run output here
	runner := runner.Runner{}
	runner.Init(srcConfig)

	runner.Run()

}
