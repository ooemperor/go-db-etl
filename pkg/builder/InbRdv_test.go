package builder

import (
	"testing"
)

func TestBuilderInbRdvSelect(t *testing.T) {
	tableName := "TestTable"
	script, err := BuildInbRdvSatCurSelect(tableName)

	if err != nil {
		t.Fatalf("Error on InbRdvSatCurSelect: %v", err)
	}

	if script != `SELECT NOW() AS load_dts, NULL AS delete_dts, decode(md5(CAST(t.* AS text)), 'hex') AS frh, t.* FROM inb.TestTable AS t;` {
		t.Fatalf("Select table statement incorrect: %v", script)
	}
}

func TestBuilderInbRdvSelectTableNameEmpty(t *testing.T) {
	tableName := ""
	_, err := BuildInbRdvSatCurSelect(tableName)

	if err == nil {
		t.Fatalf("No error has been raised for empty tableName")
	}

	if err.Error() != "the tablename cannot be blank" {
		t.Fatalf("incorrect error: %v", err.Error())
	}
}

func TestGetRdvSatCurTableName(t *testing.T) {
	tableName := "TestTable"
	tableNameSatCur, err := GetRdvSatCurTableName(tableName)

	if err != nil {
		t.Fatalf("Error on tableNameSatCur calculation: %v", err)
	}

	if tableNameSatCur != "TestTable_sat_cur" {
		t.Fatalf("sat_cur table name incorrect: %v", tableNameSatCur)
	}
}

func TestGetRdvSatTableName(t *testing.T) {
	tableName := "TestTable"
	tableNameSat, err := GetRdvSatTableName(tableName)

	if err != nil {
		t.Fatalf("Error on tableNameSat calculation: %v", err)
	}

	if tableNameSat != "TestTable_sat" {
		t.Fatalf("sat table name incorrect: %v", tableNameSat)
	}
}

func TestBuildInbRdvSatDeleteQuery(t *testing.T) {
	tableName := "TestTable"
	query, err := BuildInbRdvSatDeleteQuery(tableName)

	if err != nil {
		t.Fatalf("Error on BuildInbRdvSatDeleteQuery build: %v", err)
	}

	// if query != "UPDATE rdv.TestTable_sat SET delete_dts = NOW() WHERE frh NOT IN (SELECT frh FROM rdv.TestTable_sat_cur) AND delete_dts IS NULL;" {
	// 	t.Fatalf("BuildInbRdvSatDeleteQuery incorrect: %v", query)
	// }

	if query != "UPDATE rdv.TestTable_sat s SET delete_dts = NOW() WHERE s.delete_dts IS NULL AND NOT EXISTS (SELECT 1 FROM rdv.TestTable_sat_cur sc WHERE sc.frh = s.frh);" {
		t.Fatalf("BuildInbRdvSatDeleteQuery incorrect: %v", query)
	}
}

func TestBuildInbRdvSatInsertQuery(t *testing.T) {
	tableName := "TestTable"
	query, err := BuildInbRdvSatInsertQuery(tableName)

	if err != nil {
		t.Fatalf("Error on BuildInbRdvSatInsertQuery build: %v", err)
	}

	// if query != "INSERT INTO rdv.TestTable_sat SELECT * FROM rdv.TestTable_sat_cur WHERE frh NOT IN (SELECT frh FROM rdv.TestTable_sat);" {
	// 	t.Fatalf("BuildInbRdvSatInsertQuery incorrect: %v", query)
	// 	}
	if query != "INSERT INTO rdv.TestTable_sat SELECT sc.* FROM rdv.TestTable_sat_cur AS sc LEFT JOIN rdv.TestTable_sat AS s ON s.frh = sc.frh WHERE s.frh IS NULL;" {
		t.Fatalf("BuildInbRdvSatInsertQuery incorrect: %v", query)
	}

}
