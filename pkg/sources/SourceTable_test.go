package sources

import "testing"

/*
TestSourceTable_General Basic functionality and loading tests for the SourceTable
*/
func TestSourceTable_General(t *testing.T) {
	var srcConfig, _ = BuildSourceConfig("./test_files/src_example.json")
	srcTable := srcConfig.SrcSys[0].Tables[0]

	t.Run("SourceTableNameTest", func(t *testing.T) {
		if srcTable.Name != "Table1" {
			t.Errorf("srcTableName is incorrect = %v expected: %v", srcTable.Name, "Table1")
		}
	})

	t.Run("SourceTableSrcSysTest", func(t *testing.T) {
		if srcTable.SrcSys != "mssql1" {
			t.Errorf("SrcSys is incorrect = %v expected: %v", srcTable.SrcSys, "mssql1")
		}
	})

	t.Run("SourceTableEnabled", func(t *testing.T) {
		if srcTable.Enabled != true {
			t.Errorf("Enabled is incorrect = %v expected: %v", srcTable.Enabled, true)
		}
	})

	t.Run("SourceTableSrcFilter", func(t *testing.T) {
		if srcTable.srcFilter != "" {
			t.Errorf("srcFilter is incorrect = %v expected: %v", srcTable.srcFilter, "")
		}
	})

	t.Run("SourceTableSrcQuery", func(t *testing.T) {
		if srcTable.srcQuery != "" {
			t.Errorf("srcQuery is incorrect = %v expected: %v", srcTable.srcQuery, "")
		}
	})
}

/*
TestSourceTable_GetQuery tests the cases for retreiving the queries for the table
*/
func TestSourceTable_GetQuery(t *testing.T) {
	srcTableFilter := SourceTable{Name: "Table1", SrcSys: "sql", Enabled: true, srcFilter: "WHERE name = 'NameTest'"}
	srcTableQuery := SourceTable{Name: "Table1", SrcSys: "sql", Enabled: true, srcQuery: "Select * from public.Table1 WHERE name = 'NameTest'"}

	t.Run("SourceTableGetSrcQueryTest1", func(t *testing.T) {
		query := srcTableFilter.GetSrcQuery()
		if query != "" {
			t.Errorf("srcQuery is incorrect = %v expected: %v", query, "")
		}
	})

	t.Run("SourceTableGetSrcQueryTest2", func(t *testing.T) {
		query := srcTableQuery.GetSrcQuery()
		if query != "Select * from public.Table1 WHERE name = 'NameTest'" {
			t.Errorf("srcQuery is incorrect = %v expected: %v", query, "")
		}
	})

	t.Run("SourceTableGetSelectFilterTest1", func(t *testing.T) {
		query := srcTableQuery.GetSrcQuery()
		if query != "Select * from public.Table1 WHERE name = 'NameTest'" {
			t.Errorf("srcQuery is incorrect = %v expected: %v", query, "")
		}
	})
}

func TestSourceTable_GetSelectQuery(t *testing.T) {
	srcTableFilter := SourceTable{Name: "Table1", SrcSys: "sql", Enabled: true, srcFilter: "WHERE name = 'NameTest'"}
	srcTableQuery := SourceTable{Name: "Table1", SrcSys: "sql", Enabled: true, srcQuery: "Select * from public.Table1 WHERE name = 'NameTest' AND 1=1;"}
	srcTableNormal := SourceTable{Name: "Table1", SrcSys: "sql", Enabled: true}
	srcTableBlank := SourceTable{Name: "", SrcSys: "sql", Enabled: true}

	tests := []struct {
		name    string
		table   SourceTable
		want    string
		wantErr bool
	}{
		{name: "GetSelectQueryTest1", table: srcTableFilter, want: "SELECT * FROM Table1 WHERE name = 'NameTest';", wantErr: false},
		{name: "GetSelectQueryTest2", table: srcTableQuery, want: "Select * from public.Table1 WHERE name = 'NameTest' AND 1=1;", wantErr: false},
		{name: "GetSelectQueryTest3", table: srcTableNormal, want: "SELECT * FROM Table1;", wantErr: false},
		{name: "GetSelectQueryTest4", table: srcTableBlank, want: "", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.table.GetSelectQuery()
			if (err != nil) != tt.wantErr {
				t.Errorf("GetSelectQuery() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("GetSelectQuery() got = %v, want %v", got, tt.want)
			}
		})
	}
}
