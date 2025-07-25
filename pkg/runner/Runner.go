package runner

import (
	"fmt"
	"gihub.com/ooemperor/go-db-etl/pkg/logging"
	"gihub.com/ooemperor/go-db-etl/pkg/pipeline/inb"
	"gihub.com/ooemperor/go-db-etl/pkg/sources"
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

	sourceSystems := runner.sourceConfig.GetActiveSystems()

	for i, system := range sourceSystems {
		logging.EtlLogger.Info(fmt.Sprintf("Running system %d %v", i, system.System.Name))
		// now execute the system load.
		sysPack := inb.NewSystemPackage(system)
		err := sysPack.Build()
		if err != nil {
			logging.EtlLogger.Error("Error building package for " + system.Name)
			continue
		}
		err = sysPack.Run()

		if err != nil {
			if err != nil {
				logging.EtlLogger.Error("Error running package for " + system.Name)
				continue
			}
		}
	}

}
