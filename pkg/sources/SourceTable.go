package sources

import (
	"database/sql"
	"fmt"
	"go-db-etl/pkg/logging"
)

type SourceTable struct {
	Name      string
	SrcSys    string
	Enabled   bool
	srcFilter string
	srcQuery  string
}

/*
GetSelectQuery assembles the selectQuery for the given table.
*/
func (st *SourceTable) GetSelectQuery() (string, error) {
	if st.srcQuery != "" {
		return st.srcQuery, nil
	} else if st.srcFilter != "" {
		return fmt.Sprintf("SELECT * FROM %s %s;", st.Name, st.srcFilter), nil
	} else {
		return fmt.Sprintf("SELECT * FROM %s;", st.Name), nil
	}
}

func (st *SourceTable) GetSelectFilter() (string, error) {
	return st.srcFilter, nil
}

func (st *SourceTable) GetSrcQuery() (string, error) {
	return st.srcQuery, nil
}

/*
Load is the main loading function for Source Tables
*/
func (st *SourceTable) Load(db *sql.DB) error {

	sqlCmd, err := st.GetSelectQuery()
	if err != nil {
		logging.EtlLogger.Warning("Error while loading table " + st.Name + ": " + err.Error())
		return err
	}

	//result, err := db.Exec(sqlCmd)
	rows, err := db.Query(sqlCmd)
	defer rows.Close()
	if err != nil {
		logging.EtlLogger.Warning("Error while loading table " + st.Name + ": " + err.Error())
		return err
	}

	var rowOutput []map[string]string
	columns, _ := rows.Columns()
	// Holds raw data
	values := make([]interface{}, len(columns)) // Holds raw data
	args := make([]interface{}, len(columns))   // Pointers to values

	for i := range columns {
		args[i] = &values[i] // Each pointer will point to a value
	}

	for rows.Next() {

		// construct my
		err = rows.Scan(args...)
		if err != nil {
			logging.EtlLogger.Warning("Error while loading table " + st.Name + ": " + err.Error())
			return err
		}
		// prepare temp map to hold the result of the san
		rowData := make(map[string]string) // Dynamic row structure

		for i, col := range columns {
			var v string
			val := values[i]

			// Convert []byte to string if needed
			b, ok := val.([]byte)
			if ok {
				v = string(b)
			} else if fmt.Sprintf("%v", val) == "<nil>" {
				v = "NULL"
			} else {
				v = fmt.Sprintf("%v", val)
			}

			rowData[col] = v
		}
		fmt.Println(rowData)

		rowOutput = append(rowOutput, rowData)
	}
	query, err := st.ConstructInsertQuery(columns, rowOutput)
	if err != nil {
		logging.EtlLogger.Warning("Error while constructing the insert statement loading table " + st.Name + ": " + err.Error())
		return err
	}

	err = st.Save(query)

	if err != nil {
		logging.EtlLogger.Warning("Error while Saving table " + st.Name + ": " + err.Error())
	}
	return nil
}

/*
ConstructInsertQuery constructs the insert query for a given table
*/
func (st *SourceTable) ConstructInsertQuery(columns []string, values []map[string]string) (string, error) {

	if len(values) == 0 {
		return "", nil
	}
	columnNamesString := ""
	for i, col := range columns {
		if i == len(columns)-1 {
			columnNamesString += col
		} else {
			columnNamesString += col + ", "
		}
	}

	valuesString := ""
	for rowIndex, value := range values {
		valuesString += "("

		for colIndex, column := range columns {
			if value[column] == "NULL" {
				valuesString += "NULL"
			} else {
				valuesString += "'" + string(value[column]) + "'"
			}
			if colIndex != len(columns)-1 {
				valuesString += ","
			}
		}
		valuesString += ")"
		if rowIndex != len(values)-1 {
			valuesString += ","
		}
	}

	insertString := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s;", st.Name, columnNamesString, valuesString)
	return insertString, nil
}

func (st *SourceTable) Save(query string) error {
	// execute the insert query into the new database
	fmt.Println(query)
	if query == "" {
		// dont need to execute anything right now
	}
	return nil
}
