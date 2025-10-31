package builder

import "fmt"

/*
BuildTruncateTableSql return truncate script for a given table
*/
func BuildTruncateTableSql(schema string, tableName string) (string, error) {
	var script string

	if schema == "" {
		return "", fmt.Errorf("the schema cannot be blank")
	}
	if tableName == "" {
		return "", fmt.Errorf("the tablename cannot be blank")
	}

	script += fmt.Sprintf("TRUNCATE TABLE %s.%s;", schema, tableName)

	return script, nil
}

/*
ScriptSetTableUnlogged constructs script to make table unlogged
*/
func ScriptSetTableUnlogged(schema string, tableName string) (string, error) {
	var script string

	if schema == "" {
		return "", fmt.Errorf("the schema cannot be blank")
	}
	if tableName == "" {
		return "", fmt.Errorf("the tablename cannot be blank")
	}

	script += fmt.Sprintf("ALTER TABLE %s.%s SET UNLOGGED;", schema, tableName)
	return script, nil
}

/*
ScriptSetTableLogged constructs script to make table unlogged
*/
func ScriptSetTableLogged(schema string, tableName string) (string, error) {
	var script string

	if schema == "" {
		return "", fmt.Errorf("the schema cannot be blank")
	}
	if tableName == "" {
		return "", fmt.Errorf("the tablename cannot be blank")
	}
	script += fmt.Sprintf("ALTER TABLE %s.%s SET LOGGED;", schema, tableName)
	return script, nil
}

/*
ScriptTransactionWrapper Wraps string in transction for postgresql
*/
func ScriptTransactionWrapper(query string) string {
	prefix := "DO $$ BEGIN"
	suffix := "END $$; COMMIT;"
	return fmt.Sprintf("%v %v %v", prefix, query, suffix)
}
