package sources

import "testing"

/*
Basic functionality and loading tests for the SourceTable
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
