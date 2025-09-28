package srcinb

import (
	"database/sql"
	"testing"

	"github.com/ooemperor/go-db-etl/pkg/sources"
)

/*
TestSrcTablePipelineBuilder_getDestinationTable tests the functionality of the getDestinationTableName
*/
func TestSrcTablePipelineBuilder_getDestinationTable(t *testing.T) {
	type fields struct {
		SourceDb *sql.DB
		TargetDb *sql.DB
		Table    *sources.SourceTable
	}

	sourceTableForTest1 := &sources.SourceTable{Name: "testSourceTableForTest1", SrcSys: "testSys"}
	sourceTableForTest2 := &sources.SourceTable{Name: "testSourceTableForTest2", SrcSys: "testing"}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{name: "getDestinationTableTest1", fields: fields{nil, nil, sourceTableForTest1}, want: "testSourceTableForTest1_testSys"},
		{name: "getDestinationTableTest1", fields: fields{nil, nil, sourceTableForTest2}, want: "testSourceTableForTest2_testing"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			inb := &SrcTablePipelineBuilder{
				SourceDb: tt.fields.SourceDb,
				TargetDb: tt.fields.TargetDb,
				Table:    tt.fields.Table,
			}
			if got := inb.getDestinationTable(); got != tt.want {
				t.Errorf("getDestinationTable() = %v, want %v", got, tt.want)
			}
		})
	}
}
