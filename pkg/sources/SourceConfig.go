package sources

import (
	"encoding/json"
	"fmt"
	"go-db-etl/pkg/logging"
	"os"
)

/*
SourceConfig definition of the struct
*/
type SourceConfig struct {
	SrcSys   []*SourceSystem
	SrcTable []*SourceTable
	Target   *System
}

/*
GetActiveSystems Fetches the active systems from the source config
*/
func (sc *SourceConfig) GetActiveSystems() []*SourceSystem {
	var activeSystems []*SourceSystem
	for _, s := range sc.SrcSys {
		if s.Enabled {
			activeSystems = append(activeSystems, s)
		}
	}
	return activeSystems
}

/*
setTableForSys updates the srcTables for all the srcSystems after the initial load
*/
func (sc *SourceConfig) setTableForSys() {
	for _, sys := range sc.SrcSys {
		var tables []*SourceTable
		for _, table := range sc.SrcTable {
			if table.SrcSys == sys.Name {
				tables = append(tables, table)
			}
		}
		sys.tables = tables
	}
}

/*
BuildSourceConfig Build the configuration for the source File
*/
func BuildSourceConfig(fileName string) (*SourceConfig, error) {
	file, err := os.Open(fileName)

	if err != nil {
		logging.EtlLogger.Warning("Error on Opening srcConfig file")
		return nil, err
	}

	defer file.Close()

	decoder := json.NewDecoder(file)
	configuration := SourceConfig{}
	err = decoder.Decode(&configuration)
	if err != nil {
		fmt.Println("error:", err)
	}

	configuration.setTableForSys()

	return &configuration, nil
}
