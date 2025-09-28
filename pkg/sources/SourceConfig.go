package sources

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/ooemperor/go-db-etl/pkg/logging"
)

/*
SourceConfig definition of the struct
*/
type SourceConfig struct {
	SrcSys []*System
	Target *System
}

/*
GetActiveSystems Fetches the active systems from the source config
*/
func (sc *SourceConfig) GetActiveSystems() []*System {
	var activeSystems []*System
	for _, s := range sc.SrcSys {
		if s.Enabled {
			activeSystems = append(activeSystems, s)
		}
	}
	return activeSystems
}

/*
BuildSourceConfig Build the configuration for the source File
*/
func BuildSourceConfig(fileName string) (*SourceConfig, error) {
	file, err := os.ReadFile(fileName)
	if err != nil {
		logging.EtlLogger.Warning("Error on Opening srcConfig file")
		return nil, err
	}

	configuration := SourceConfig{}
	err = json.Unmarshal(file, &configuration)
	if err != nil {
		fmt.Println("error:", err)
	}
	return &configuration, nil
}
