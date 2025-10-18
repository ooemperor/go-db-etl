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
	EtlLogLevel     int
	RunSrcInb       bool
	RunInbRdv       bool
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
	conf.timeout, _ = strconv.ParseInt(os.Getenv("TIMEOUT_SEC"), 10, 64)
	conf.Name = os.Getenv("name")
	var batchReader, _ = strconv.ParseInt(os.Getenv("BATCH_SIZE_READ"), 10, 64)
	var batchWriter, _ = strconv.ParseInt(os.Getenv("BATCH_SIZE_WRITER"), 10, 64)

	conf.BatchSizeReader = int(batchReader)
	conf.BatchSizeWriter = int(batchWriter)

	conf.RunSrcInb, _ = strconv.ParseBool(os.Getenv("RUN_SRCINB"))
	conf.RunInbRdv, _ = strconv.ParseBool(os.Getenv("RUN_INBRDV"))
	conf.EtlLogLevel, _ = strconv.Atoi(os.Getenv("ETL_LOGLEVEL"))

	return conf, nil
}

/*
Config Export the Config object
*/
var Config, _ = (&(Configuration{})).Init()

var SourceConfiguration, _ = sources.BuildSourceConfig("src.json")
