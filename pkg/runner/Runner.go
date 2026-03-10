package runner

import (
	"fmt"
	"sync"

	"github.com/ooemperor/go-db-etl/pkg/config"
	"github.com/ooemperor/go-db-etl/pkg/logging"
	"github.com/ooemperor/go-db-etl/pkg/packages/inbrdv"
	"github.com/ooemperor/go-db-etl/pkg/packages/srcinb"
	"github.com/ooemperor/go-db-etl/pkg/sources"
	"github.com/teambenny/goetl/logger"
)

/*
Runner defines the object of the runner that will go and execute all the Operations
*/
type Runner struct {
	sourceConfig   *sources.SourceConfig
	srcInbPackages []*srcinb.SystemPackage
	inbRdvPackages []*inbrdv.InbPackage
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
	if config.Config.RunSrcInb {
		runner.runSrcInb()
	} else {
		logging.EtlLogger.Info("SKIP SrcInb due to config")
	}

	if config.Config.RunInbRdv {
		runner.runInbRdv()
	} else {
		logging.EtlLogger.Info("SKIP InbRdv due to config")
	}

	if config.Config.RunSrcInb {
		runner.RunAdditionalCommands()
	} else {
		logging.EtlLogger.Info("SKIP AdditionalCommands due to config")
	}
}

/*
runSrcInb executes the prebuilt SrcInb packages
*/
func (runner *Runner) runSrcInb() {
	logging.EtlLogger.Info("START Running systems")
	logging.EtlLogger.Info("START SrcInb")

	var wg sync.WaitGroup
	defer wg.Wait()
	wg.Add(len(runner.srcInbPackages))

	for i, srcInbPackage := range runner.srcInbPackages {
		logging.EtlLogger.Info(fmt.Sprintf("Running srcinb for system %d %v", i, srcInbPackage.Name()))
		var exec = func() {
			err := srcInbPackage.Run()
			if err != nil {
				logging.EtlLogger.Error("Error running srcinb for package: " + srcInbPackage.Name())
			}
			wg.Done()
		}
		if config.Config.RunSrcInbParallel {
			go exec()
		} else {
			exec()
		}
	}
	logging.EtlLogger.Info("END SrcInb")
}

/*
runInbRdv executes the prebuilt inbRdv packages
*/
func (runner *Runner) runInbRdv() {
	logging.EtlLogger.Info("START InbRdv")

	var wg sync.WaitGroup
	defer wg.Wait()
	wg.Add(len(runner.inbRdvPackages))

	for i, inbRdvPack := range runner.inbRdvPackages {
		logging.EtlLogger.Info(fmt.Sprintf("Running srcinb for system %d %v", i, inbRdvPack.Name()))
		var exec = func() {
			err := inbRdvPack.Run()
			if err != nil {
				logging.EtlLogger.Error("Error running srcinb for package: " + inbRdvPack.Name())
			}
			wg.Done()
		}
		if config.Config.RunInbRdvParallel {
			go exec()
		} else {
			exec()
		}
	}

	logging.EtlLogger.Info("END InbRdv")
	logging.EtlLogger.Info("END Running systems")
}

func (runner *Runner) RunAdditionalCommands() {
	logging.EtlLogger.Info("START Additional Commands")
	db, err := runner.sourceConfig.Target.GetDB()
	if err != nil {
		logging.EtlLogger.Error("Error getting DB connection for additional commands: " + err.Error())
		return
	}
	logging.EtlLogger.Info(fmt.Sprintf("Running %d additional commands", len(runner.sourceConfig.AdditionalCommands)))
	for _, cmd := range runner.sourceConfig.AdditionalCommands {
		logging.EtlLogger.Info(fmt.Sprintf("Running additional command: %s", *cmd))
		_, err := db.Exec(*cmd)
		if err != nil {
			logging.EtlLogger.Error("Error running additional command: " + *cmd + " Error: " + err.Error())
		}
	}
	logging.EtlLogger.Info("END Additional Commands")
}

/*
reset clears all the built packages of the object
*/
func (runner *Runner) reset() {
	runner.srcInbPackages = make([]*srcinb.SystemPackage, 0)
	runner.inbRdvPackages = make([]*inbrdv.InbPackage, 0)
	logging.EtlLogger.Info("RESET of the RUNNER DONE")
}

func (runner *Runner) Build() {
	// Build the packages to run them later
	// setting the loglevel of the goetl module
	logger.LogLevel = config.Config.EtlLogLevel
	if runner.sourceConfig == nil {
		logging.EtlLogger.Error("No sourceConfig defined for this runner")
	}
	runner.reset()
	runner.buildSystems()
}

/*
buildSystems builds all the srcInb and inbRdv Packages
*/
func (runner *Runner) buildSystems() {
	sourceSystems := runner.sourceConfig.GetActiveSystems()

	logging.EtlLogger.Info("START Building systems")
	for i, system := range sourceSystems {
		logging.EtlLogger.Info(fmt.Sprintf("Building system %d %v", i, system.Name))
		// now execute the system load.
		sysPack := srcinb.NewSystemPackage(system, runner.sourceConfig.Target)
		err := sysPack.Build()
		runner.srcInbPackages = append(runner.srcInbPackages, sysPack)
		if err != nil {
			logging.EtlLogger.Error("Error building srcinb package for " + system.Name)
			continue
		}

		inbRdvPack := inbrdv.NewInbRdvPackage(system, runner.sourceConfig.Target)
		err = inbRdvPack.Build()
		runner.inbRdvPackages = append(runner.inbRdvPackages, inbRdvPack)
		if err != nil {
			logging.EtlLogger.Error("Error building inbrdv package for " + system.Name)
			continue
		}
	}

	logging.EtlLogger.Info("END Building systems")
}
