package runner

import (
	"fmt"

	"github.com/ooemperor/go-db-etl/pkg/logging"
	"github.com/ooemperor/go-db-etl/pkg/packages/inbrdv"
	"github.com/ooemperor/go-db-etl/pkg/packages/srcinb"
	"github.com/ooemperor/go-db-etl/pkg/sources"
)

/*
Runner defines the object of the runner that will go and execute all the Operations
*/
type Runner struct {
	sourceConfig *sources.SourceConfig
}

/*
Init initilizes the runner with all its attributes
*/
func (runner *Runner) Init(sourceConfig *sources.SourceConfig) {
	runner.sourceConfig = sourceConfig
}

/*
Run is the main function that will go ahead and executes all the operations as defined by the source config
*/
func (runner *Runner) Run() {
	if runner.sourceConfig == nil {
		logging.EtlLogger.Error("No sourceConfig defined for this runner")
	}

	// setting the loglevel of the goetl module
	// logger.LogLevel = 4

	sourceSystems := runner.sourceConfig.GetActiveSystems()

	sysPackages := make([]*srcinb.SystemPackage, 0)
	inbRdvPackages := make([]*inbrdv.InbPackage, 0)

	logging.EtlLogger.Info("START Building systems")
	for i, system := range sourceSystems {
		logging.EtlLogger.Info(fmt.Sprintf("Building system %d %v", i, system.Name))
		// now execute the system load.
		sysPack := srcinb.NewSystemPackage(system, runner.sourceConfig.Target)
		err := sysPack.Build()
		sysPackages = append(sysPackages, sysPack)
		if err != nil {
			logging.EtlLogger.Error("Error building srcinb package for " + system.Name)
			continue
		}

		inbRdvPack := inbrdv.NewInbRdvPackage(system, runner.sourceConfig.Target)
		err = inbRdvPack.Build()
		inbRdvPackages = append(inbRdvPackages, inbRdvPack)
		if err != nil {
			logging.EtlLogger.Error("Error building inbrdv package for " + system.Name)
			continue
		}
	}

	logging.EtlLogger.Info("END Building systems")
	logging.EtlLogger.Info("START Running systems")
	logging.EtlLogger.Info("START SrcInb")

	for i, sysPackage := range sysPackages {
		logging.EtlLogger.Info(fmt.Sprintf("Running srcinb for system %d %v", i, sysPackage.Name()))
		err := sysPackage.Run()
		if err != nil {
			logging.EtlLogger.Error("Error running srcinb for package: " + sysPackage.Name())
			continue
		}
	}
	logging.EtlLogger.Info("END SrcInb")
	logging.EtlLogger.Info("START InbRdv")

	for i, inbRdvPack := range inbRdvPackages {
		logging.EtlLogger.Info(fmt.Sprintf("Running srcinb for system %d %v", i, inbRdvPack.Name()))
		err := inbRdvPack.Run()
		if err != nil {
			logging.EtlLogger.Error("Error running srcinb for package: " + inbRdvPack.Name())
			continue
		}
	}
	
	logging.EtlLogger.Info("END InbRdv")
	logging.EtlLogger.Info("END Running systems")
}
