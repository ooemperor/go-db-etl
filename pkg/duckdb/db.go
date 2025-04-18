package duckdb

import (
	"database/sql"
	"go-db-etl/pkg/logging"
)

func setupDB() {
	// create database in memory
	db, err := sql.Open("duckdb", "")

	if err != nil {
		logging.EtlLogger.Error("Error opening DB")
	}

	defer func(db *sql.DB) {
		err := db.Close()
		if err != nil {
			logging.EtlLogger.Error(err.Error())
		}
	}(db)
}
