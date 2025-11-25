package srcinb

import (
	"database/sql"
	"reflect"
	"testing"

	"github.com/ooemperor/go-db-etl/pkg/config"
	"github.com/ooemperor/go-db-etl/pkg/sources"
	"github.com/teambenny/goetl"
	"github.com/teambenny/goetl/processors"
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

/*
TestSrcTablePipelineBuilder_buildSrcInbTruncator tests the truncator stage build phase for the srcinb load
*/
func TestSrcTablePipelineBuilder_buildSrcInbTruncator(t *testing.T) {
	type fields struct {
		SourceDb *sql.DB
		TargetDb *sql.DB
		Table    *sources.SourceTable
	}
	srcTable1 := sources.SourceTable{Name: "InbTable", SrcSys: "sys", Enabled: true}
	srcTable2 := sources.SourceTable{Name: "", SrcSys: "sys", Enabled: true}
	tests := []struct {
		name    string
		fields  fields
		want    *processors.SQLExecutor
		wantErr bool
	}{
		{name: "SrcInbTruncatorBuildTest1", fields: fields{nil, nil, &srcTable1}, want: processors.NewSQLExecutor(nil, "TRUNCATE TABLE inb.InbTable_sys;"), wantErr: false},
		{name: "SrcInbTruncatorBuildTest2", fields: fields{nil, nil, &srcTable2}, want: nil, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			inb := &SrcTablePipelineBuilder{
				SourceDb: tt.fields.SourceDb,
				TargetDb: tt.fields.TargetDb,
				Table:    tt.fields.Table,
			}
			got, err := inb.buildSrcInbTruncator()
			if (err != nil) != tt.wantErr {
				t.Errorf("buildSrcInbTruncator() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("buildSrcInbTruncator() got = %v, want %v", got, tt.want)
			}
		})
	}
}

/*
TestSrcTablePipelineBuilder_buildSrcInbReader tests the reader stage build phase for the srcinb load
*/
func TestSrcTablePipelineBuilder_buildSrcInbReader(t *testing.T) {
	type fields struct {
		SourceDb *sql.DB
		TargetDb *sql.DB
		Table    *sources.SourceTable
	}
	srcTable1 := sources.SourceTable{Name: "InbTable", SrcSys: "sys", Enabled: true}
	srcTable2 := sources.SourceTable{Name: "", SrcSys: "sys", Enabled: true}

	reader1 := processors.NewSQLReader(nil, "SELECT * FROM InbTable;")
	reader1.BatchSize = config.Config.BatchSizeReader

	tests := []struct {
		name    string
		fields  fields
		want    *processors.SQLReader
		wantErr bool
	}{
		{name: "SrcInbReaderBuildTest1", fields: fields{nil, nil, &srcTable1}, want: nil, wantErr: true},
		{name: "SrcInbReaderBuildTest2", fields: fields{nil, nil, &srcTable2}, want: nil, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			inb := &SrcTablePipelineBuilder{
				SourceDb: tt.fields.SourceDb,
				TargetDb: tt.fields.TargetDb,
				Table:    tt.fields.Table,
			}
			_, err := inb.buildSrcInbReader()
			if (err != nil) != tt.wantErr {
				t.Errorf("buildSrcInbReader() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}

/*
TestSrcTablePipelineBuilder_buildSrcInbWriter tests the writer stage build phase for the srcinb load
*/
func TestSrcTablePipelineBuilder_buildSrcInbWriter(t *testing.T) {
	type fields struct {
		SourceDb *sql.DB
		TargetDb *sql.DB
		Table    *sources.SourceTable
	}
	srcTable1 := sources.SourceTable{Name: "InbTable", SrcSys: "sys", Enabled: true}
	srcTable2 := sources.SourceTable{Name: "", SrcSys: "sys", Enabled: true}

	writer1 := processors.NewPostgreSQLWriter(nil, "InbTable_sys")
	writer1.BatchSize = config.Config.BatchSizeWriter
	writer1.OnDupKeyUpdate = false

	tests := []struct {
		name    string
		fields  fields
		want    *processors.PostgreSQLWriter
		wantErr bool
	}{
		{name: "SrcInbWriterBuildTest1", fields: fields{nil, nil, &srcTable1}, want: writer1, wantErr: false},
		{name: "SrcInbWriterBuildTest2", fields: fields{nil, nil, &srcTable2}, want: nil, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			inb := &SrcTablePipelineBuilder{
				SourceDb: tt.fields.SourceDb,
				TargetDb: tt.fields.TargetDb,
				Table:    tt.fields.Table,
			}
			got, err := inb.buildSrcInbWriter()
			if (err != nil) != tt.wantErr {
				t.Errorf("buildSrcInbWriter() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("buildSrcInbWriter() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSrcTablePipelineBuilder_Build(t *testing.T) {
	type fields struct {
		SourceDb *sql.DB
		TargetDb *sql.DB
		Table    *sources.SourceTable
	}
	srcTable1 := sources.SourceTable{Name: "InbTable", SrcSys: "sys", Enabled: true}
	srcTable2 := sources.SourceTable{Name: "", SrcSys: "sys", Enabled: true}
	tests := []struct {
		name    string
		fields  fields
		want    *goetl.Pipeline
		wantErr bool
	}{
		{name: "SrcInbBuildTest1", fields: fields{nil, nil, &srcTable1}, want: nil, wantErr: true},
		{name: "SrcInbBuildTest2", fields: fields{nil, nil, &srcTable2}, want: nil, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			inb := &SrcTablePipelineBuilder{
				SourceDb: tt.fields.SourceDb,
				TargetDb: tt.fields.TargetDb,
				Table:    tt.fields.Table,
			}
			_, err := inb.Build()
			if (err != nil) != tt.wantErr {
				t.Errorf("Build() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}
