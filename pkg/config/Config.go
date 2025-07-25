package config

import (
	"github.com/joho/godotenv"
	"github.com/ooemperor/go-db-etl/pkg/sources"
	"os"
	"strconv"
)

/*
Configuration Object that holds all the necessary config information
*/
type Configuration struct {
	postgresHost string
	timeout      int64
	Name         string
}

/*
Init initializes the config parameters
*/
func (conf *Configuration) Init(envFile string) (*Configuration, error) {
	err := godotenv.Load(envFile)
	if err != nil {
		return conf, err
	}

	conf.timeout, _ = strconv.ParseInt(os.Getenv("timeout_sec"), 10, 64)
	conf.Name = os.Getenv("name")

	return conf, nil
}

/*
Config Export the Config object
*/
var Config, _ = (&(Configuration{})).Init(".env")

var SourceConfiguration, _ = sources.BuildSourceConfig("src.json")
