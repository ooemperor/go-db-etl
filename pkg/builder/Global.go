package builder

import "fmt"

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
