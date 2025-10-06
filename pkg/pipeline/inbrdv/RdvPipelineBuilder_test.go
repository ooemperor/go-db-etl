package inbrdv

import (
	"database/sql"
	"reflect"
	"testing"

	"github.com/ooemperor/go-db-etl/pkg/config"
	"github.com/teambenny/goetl"
	"github.com/teambenny/goetl/processors"
)

/*
TestRdvPipeline_buildTruncator for verification of the funtionality of the truncatorBuilder from the targetDB
*/
func TestRdvPipeline_buildTruncator(t *testing.T) {
	type fields struct {
		db    *sql.DB
		Table string
	}
	tests := []struct {
		name    string
		fields  fields
		want    *processors.SQLExecutor
		wantErr bool
	}{
		{name: "TruncatorTest1", fields: fields{nil, ""}, want: nil, wantErr: true},
		{name: "TruncatorTest2", fields: fields{nil, "testTable"}, want: processors.NewSQLExecutor(nil, "TRUNCATE TABLE rdv.testTable;"), wantErr: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rdv := &RdvPipeline{
				Db:    tt.fields.db,
				Table: tt.fields.Table,
			}
			got, err := rdv.buildTruncator()
			if (err != nil) != tt.wantErr {
				t.Errorf("buildTruncator() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("buildTruncator() got = %v, want %v", got, tt.want)
			}
		})
	}
}

/*
TestRdvPipeline_buildInbReader for verification of the funtionality of the buildInbReader from the targetDB
*/
func TestRdvPipeline_buildInbReader(t *testing.T) {
	type fields struct {
		db    *sql.DB
		Table string
	}

	expectedReaderForTest2 := processors.NewSQLReader(nil, "SELECT NOW(), NULL, decode(md5(CAST(t.* AS text)), ''hex''), t.* FROM inb.testTableInsert AS t;")
	expectedReaderForTest2.BatchSize = config.Config.BatchSizeReader
	tests := []struct {
		name    string
		fields  fields
		want    *processors.SQLReader
		wantErr bool
	}{
		{name: "InbReaderTest1", fields: fields{nil, ""}, want: nil, wantErr: true},
		{name: "InbReaderTest2", fields: fields{nil, "testTableInsert"}, want: expectedReaderForTest2, wantErr: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rdv := &RdvPipeline{
				Db:    tt.fields.db,
				Table: tt.fields.Table,
			}
			got, err := rdv.buildInbReader()
			if (err != nil) != tt.wantErr {
				t.Errorf("buildInbReader() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("buildInbReader() got = %v, want %v", got, tt.want)
			}
		})
	}
}

/*
TestRdvPipeline_buildSatCurWriter for verification of the funtionality of the buildSatCurWriter to the targetDB
*/
func TestRdvPipeline_buildSatCurWriter(t *testing.T) {
	type fields struct {
		db    *sql.DB
		Table string
	}

	expectedWriterForTest2 := processors.NewPostgreSQLWriter(nil, "rdv.testTableInsert_sat_cur")
	expectedWriterForTest2.BatchSize = config.Config.BatchSizeWriter
	expectedWriterForTest2.OnDupKeyUpdate = false

	tests := []struct {
		name    string
		fields  fields
		want    *processors.PostgreSQLWriter
		wantErr bool
	}{
		{name: "SatCurWriterTest1", fields: fields{nil, ""}, want: nil, wantErr: true},
		{name: "SatCurWriterTest2", fields: fields{nil, "testTableInsert"}, want: expectedWriterForTest2, wantErr: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rdv := &RdvPipeline{
				Db:    tt.fields.db,
				Table: tt.fields.Table,
			}
			got, err := rdv.buildSatCurWriter()
			if (err != nil) != tt.wantErr {
				t.Errorf("buildSatCurWriter() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("buildSatCurWriter() got = %v, want %v", got, tt.want)
			}
		})
	}
}

/*
TestRdvPipeline_buildSatMarkDelete for verification of the funtionality of the delete in the sat table of rdv to the targetDB
*/
func TestRdvPipeline_buildSatMarkDelete(t *testing.T) {
	type fields struct {
		db    *sql.DB
		Table string
	}
	tests := []struct {
		name    string
		fields  fields
		want    *processors.SQLExecutor
		wantErr bool
	}{
		{name: "SatCurDeleteTest1", fields: fields{nil, ""}, want: nil, wantErr: true},
		{name: "SatCurDeleteTest2", fields: fields{nil, "testTableDelete"}, want: processors.NewSQLExecutor(nil, "UPDATE rdv.testTableDelete_sat SET delete_dts = NOW() WHERE frh NOT IN (SELECT frh FROM rdv.testTableDelete_sat_cur) AND delete_dts IS NULL;"), wantErr: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rdv := &RdvPipeline{
				Db:    tt.fields.db,
				Table: tt.fields.Table,
			}
			got, err := rdv.buildSatMarkDelete()
			if (err != nil) != tt.wantErr {
				t.Errorf("buildSatMarkDelete() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("buildSatMarkDelete() got = %v, want %v", got, tt.want)
			}
		})
	}
}

/*
TestRdvPipeline_buildSatInsertExecutor validates the functionality of the SatInsertBuilder
*/
func TestRdvPipeline_buildSatInsertExecutor(t *testing.T) {
	type fields struct {
		db    *sql.DB
		Table string
	}
	tests := []struct {
		name    string
		fields  fields
		want    *processors.SQLExecutor
		wantErr bool
	}{
		{name: "SatInsertTest1", fields: fields{nil, ""}, want: nil, wantErr: true},
		{name: "SatInsertTest2", fields: fields{nil, "testTableSatInsert"}, want: processors.NewSQLExecutor(nil, "INSERT INTO rdv.testTableSatInsert_sat SELECT * FROM rdv.testTableSatInsert_sat_cur WHERE frh NOT IN (SELECT frh FROM rdv.testTableSatInsert_sat);"), wantErr: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rdv := &RdvPipeline{
				Db:    tt.fields.db,
				Table: tt.fields.Table,
			}
			got, err := rdv.buildSatInsertExecutor()
			if (err != nil) != tt.wantErr {
				t.Errorf("buildSatInsertExecutor() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("buildSatInsertExecutor() got = %v, want %v", got, tt.want)
			}
		})
	}
}

/*
TestRdvPipeline_Build tests if error is thrown in the RdvPipelineBuild
*/
func TestRdvPipeline_Build(t *testing.T) {
	type fields struct {
		db    *sql.DB
		Table string
	}
	tests := []struct {
		name    string
		fields  fields
		want    *goetl.Pipeline
		wantErr bool
	}{

		{name: "PipelineBuilderTest1", fields: fields{nil, ""}, want: nil, wantErr: true},
		{name: "PipelineBuilderTest1", fields: fields{nil, "PipelineBuild"}, want: nil, wantErr: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rdv := &RdvPipeline{
				Db:    tt.fields.db,
				Table: tt.fields.Table,
			}
			_, err := rdv.Build()
			if (err != nil) != tt.wantErr {
				t.Errorf("Build() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}
