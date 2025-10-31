package builder

import (
	"database/sql"
	"fmt"

	"github.com/teambenny/goetl"
	"github.com/teambenny/goetl/processors"
)

/*
BuildInbRdvSatCurSelect builds the query for getting the values from inb with hash calculation.
*/
func BuildInbRdvSatCurSelect(tableName string) (string, error) {
	var script string
	if tableName == "" {
		return "", fmt.Errorf("the tablename cannot be blank")
	}

	script += "SELECT NOW() AS load_dts, NULL AS delete_dts, decode(md5(CAST(t.* AS text)), 'hex') AS frh, t.* "
	script += fmt.Sprintf("FROM inb.%s AS t;", tableName)

	return script, nil
}

/*
GetRdvSatCurTableName returns the sat_cur name in rdv for the given table
*/
func GetRdvSatCurTableName(tableName string) (string, error) {
	if tableName == "" {
		return "", fmt.Errorf("the tablename cannot be blank")
	}

	return fmt.Sprintf("%s_sat_cur", tableName), nil
}

/*
GetRdvSatTableName returns the sat name in rdv for the given table
*/
func GetRdvSatTableName(tableName string) (string, error) {
	if tableName == "" {
		return "", fmt.Errorf("the tablename cannot be blank")
	}

	return fmt.Sprintf("%s_sat", tableName), nil
}

/*
BuildInbRdvSatDeleteQuery Constructs the query used to mark the records in sat as deleted that are no longer found
*/
func BuildInbRdvSatDeleteQuery(tableName string) (string, error) {
	var script string
	if tableName == "" {
		return "", fmt.Errorf("the tablename cannot be blank")
	}
	script += fmt.Sprintf("UPDATE rdv.%s_sat s ", tableName)
	script += "SET delete_dts = NOW() "
	script += fmt.Sprintf("WHERE s.delete_dts IS NULL AND NOT EXISTS (SELECT 1 FROM rdv.%s_sat_cur sc WHERE sc.frh = s.frh);", tableName)
	// script += fmt.Sprintf("FROM rdv.%s_sat AS s ", tableName)
	// script += fmt.Sprintf("LEFT JOIN rdv.%s_sat_cur AS sc on s.frh = sc.frh ", tableName)
	// script += "WHERE sc.frh IS NULL AND s.delete_dts IS NULL;"
	// script += fmt.Sprintf("WHERE frh NOT IN (SELECT frh FROM rdv.%s_sat_cur) ", tableName)
	// script += "AND delete_dts IS NULL;"

	return script, nil
}

/*
BuildInbRdvSatInsertQuery Constructs the query used to calculate and insert the new entries for rdv.
*/
func BuildInbRdvSatInsertQuery(tableName string) (string, error) {
	var script string
	if tableName == "" {
		return "", fmt.Errorf("the tablename cannot be blank")
	}

	script += fmt.Sprintf("INSERT INTO rdv.%s_sat ", tableName)
	script += fmt.Sprintf("SELECT sc.* FROM rdv.%s_sat_cur AS sc LEFT JOIN rdv.%s_sat AS s ON s.frh = sc.frh ", tableName, tableName)
	script += "WHERE s.frh IS NULL;"
	// script += fmt.Sprintf("WHERE frh NOT IN (SELECT frh FROM rdv.%s_sat);", tableName)

	return script, nil
}

func BuildInbRdvDummySelect(db *sql.DB) goetl.Processor {
	script := "SELECT 1"
	processor := processors.NewSQLReader(db, script)
	return processor
}
