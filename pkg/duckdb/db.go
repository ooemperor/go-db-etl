package duckdb

import (
	"database/sql"
	_ "github.com/marcboeker/go-duckdb/v2"
	"go-db-etl/pkg/logging"
)

func SetupDB() *sql.DB {
	// create database in memory
	db, err := sql.Open("duckdb", "")

	if err != nil {
		logging.EtlLogger.Error("Error opening DB" + err.Error())
	}

	return db
}
