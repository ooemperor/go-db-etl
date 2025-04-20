package config

import (
	"database/sql"
	"errors"
	"fmt"
	_ "github.com/jackc/pgx"
	_ "github.com/microsoft/go-mssqldb"
	"go-db-etl/pkg/logging"
)

type SourceSystem struct {
	Name     string
	Enabled  bool
	Username string
	Password string
	Driver   string
	Address  string
	Port     int
	Database string
	tables   []*SourceTable
}

/*
GetAllTables return the list of srcTables associated with this sourceSystem
*/
func (sys *SourceSystem) GetAllTables() ([]*SourceTable, error) {
	return sys.tables, nil
}

/*
GetActiveTables return the list of srcTables associated with this sourceSystem
*/
func (sys *SourceSystem) GetActiveTables() ([]*SourceTable, error) {
	var outputTables []*SourceTable
	for _, t := range sys.tables {
		if t.Enabled {
			outputTables = append(outputTables, t)
		}
	}
	return outputTables, nil
}

/*
GetConnectionString assembles the connection string for the given Source system
*/
func (sys *SourceSystem) GetConnectionString() (string, error) {
	switch sys.Driver {
	case "mssql":
		// sqlserver://username:password@host/instance?param1=value&param2=value
		return fmt.Sprintf("sqlserver://%s:%s@%s:%d?database=%s", sys.Username, sys.Password, sys.Address, sys.Port, sys.Database), nil

	case "postgres":
		// "postgres://username:password@localhost:5432/database_name"
		return fmt.Sprintf("postgres://%s:%s@%s:%d/%s", sys.Username, sys.Password, sys.Address, sys.Port, sys.Database), nil

	default:
		return "", errors.New("unsupported driver")
	}

}

/*
Load function to later fetch all the records from the given sourceSystem
*/
func (sys *SourceSystem) Load() error {
	logging.EtlLogger.Info("Loading Source System" + sys.Name)
	db, err := sql.Open(sys.Driver, sys.Address)

	if err != nil {
		logging.EtlLogger.Error("Error connecting to database" + sys.Name + ": " + err.Error())
	}

	defer func(db *sql.DB) {
		err := db.Close()
		if err != nil {
			logging.EtlLogger.Warning("Error while closing connection to: " + sys.Name)
		}
	}(db)

	// loop over all the specified tables and load them async
	logging.EtlLogger.Warning("Error while closing connection to: " + sys.Name)
	activeTables, _ := sys.GetAllTables()
	tableCount := len(activeTables)
	tableErrors := 0

	for i, table := range activeTables {
		logging.EtlLogger.Info(fmt.Sprintf("[%d / %d]Loading table %s", i, tableCount, table.Name))

		sqlCmd, err := table.GetSelectQuery()
		if err != nil {
			tableErrors += 1
			logging.EtlLogger.Warning("Error while loading table " + table.Name + ": " + err.Error())
			continue
		}

		//result, err := db.Exec(sqlCmd)
		_, err = db.Query(sqlCmd)

		if err != nil {
			tableErrors += 1
			logging.EtlLogger.Warning("Error while loading table " + table.Name + ": " + err.Error())
		}

		// TODO: now need to define the processing of the result.

	}
	if tableErrors > 0 {
		return errors.New(fmt.Sprintf("%s: Error while loading table in %d of %d tables", sys.Name, tableErrors, tableCount))
	}
	return nil
}
