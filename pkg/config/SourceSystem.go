package config

import (
	"database/sql"
	"errors"
	"fmt"
	_ "github.com/lib/pq"
	_ "github.com/microsoft/go-mssqldb"
	"go-db-etl/pkg/logging"
)

/*
SourceSystem object that wraps the System Object
*/
type SourceSystem struct {
	*System
}

func (sys *SourceSystem) Run() error {
	// run all the tables
	return nil
}

/*
Load function to later fetch all the records from the given sourceSystem
*/
func (sys *SourceSystem) Load() error {
	logging.EtlLogger.Info("Loading Source System " + sys.Name)
	connectionString, _ := sys.GetConnectionString()
	db, err := sql.Open(sys.Driver, connectionString)

	if err != nil {
		logging.EtlLogger.Error("Error connecting to database " + sys.Name + ": " + err.Error())
	}

	defer func(db *sql.DB) {
		err := db.Close()
		if err != nil {
			logging.EtlLogger.Warning("Error while closing connection to: " + sys.Name + ": " + err.Error())
		}
	}(db)

	// loop over all the specified tables and load them async
	activeTables, _ := sys.GetAllTables()
	tableCount := len(activeTables)
	tableErrors := 0

	for i, table := range activeTables {
		logging.EtlLogger.Info(fmt.Sprintf("[%d / %d] Loading table %s", i, tableCount, table.Name))

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
		// TODO: then insert into target

	}

	if tableErrors > 0 {
		return errors.New(fmt.Sprintf("%s: Error while loading table in %d of %d tables", sys.Name, tableErrors, tableCount))
	} else {
		logging.EtlLogger.Info("Finished Loading Source System " + sys.Name + " for " + fmt.Sprint(tableCount) + " tables")
	}
	return nil
}
