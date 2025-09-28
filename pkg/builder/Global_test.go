package builder

import (
	"testing"
)

func TestBuilderGlobalInsert(t *testing.T) {
	schemaName := "inb"
	tableName := "TestTable"
	script, err := BuildTruncateTableSql(schemaName, tableName)

	if err != nil {
		t.Fatalf("Error on BuildTruncateTableSql: %v", err)
	}

	if script != "TRUNCATE TABLE inb.TestTable;" {
		t.Fatalf("Truncate table statement incorrect: %v", script)
	}
}

func TestBuilderGlobalInsertSchemaEmpty(t *testing.T) {
	schemaName := ""
	tableName := "TestTable"
	_, err := BuildTruncateTableSql(schemaName, tableName)

	if err == nil {
		t.Fatalf("No error has been raised for empty schema")
	}

	if err.Error() != "the schema cannot be blank" {
		t.Fatalf("incorrect error: %v", err.Error())
	}
}
