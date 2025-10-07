package config

import (
	"os"
	"strconv"

	"github.com/joho/godotenv"
	"github.com/ooemperor/go-db-etl/pkg/sources"
)

/*
Configuration Object that holds all the necessary config information
*/
type Configuration struct {
	timeout         int64
	Name            string
	BatchSizeReader int
	BatchSizeWriter int
}

/*
Init initializes the config parameters
*/
func (conf *Configuration) Init() (*Configuration, error) {
	_ = godotenv.Load()
	_ = godotenv.Load(".env")
	_ = godotenv.Load("../.env")
	_ = godotenv.Load("../../.env")
	_ = godotenv.Load("../../../.env")
	conf.timeout, _ = strconv.ParseInt(os.Getenv("timeout_sec"), 10, 64)
	conf.Name = os.Getenv("name")
	var batchReader, _ = strconv.ParseInt(os.Getenv("batch_size_read"), 10, 64)
	var batchWriter, _ = strconv.ParseInt(os.Getenv("batch_size_writer"), 10, 64)

	conf.BatchSizeReader = int(batchReader)
	conf.BatchSizeWriter = int(batchWriter)

	return conf, nil
}

/*
Config Export the Config object
*/
var Config, _ = (&(Configuration{})).Init()

var SourceConfiguration, _ = sources.BuildSourceConfig("src.json")
