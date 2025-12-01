package srcinb

import (
	"database/sql"
	"fmt"

	"github.com/ooemperor/go-db-etl/pkg/builder"
	"github.com/ooemperor/go-db-etl/pkg/config"
	"github.com/ooemperor/go-db-etl/pkg/logging"
	proc "github.com/ooemperor/go-db-etl/pkg/processors"
	"github.com/ooemperor/go-db-etl/pkg/sources"
	"github.com/teambenny/goetl"
	"github.com/teambenny/goetl/processors"
)

/*
SrcTablePipelineBuilder constructs a simple data Pipeline for loading a single table.
*/
type SrcTablePipelineBuilder struct {
	SourceDb *sql.DB
	TargetDb *sql.DB
	Table    *sources.SourceTable
	Driver   string
	Address  string
}

/*
getDestinationTable return the destinationTableName for the inb layer
*/
func (inb *SrcTablePipelineBuilder) getDestinationTable() string {
	if inb.Table.Name == "" || inb.Table.SrcSys == "" {
		return ""
	}
	return inb.Table.Name + "_" + inb.Table.SrcSys
}

/*
buildSrcInbTruncator build the truncation stage for inb table
*/
func (inb *SrcTablePipelineBuilder) buildSrcInbTruncator() (*processors.SQLExecutor, error) {
	destinationTable := inb.getDestinationTable()
	truncateQuery, err := builder.BuildTruncateTableSql("inb", destinationTable)
	if err != nil {
		return nil, err
	}
	truncator := processors.NewSQLExecutor(inb.TargetDb, truncateQuery)
	return truncator, nil
}

/*
buildSrcInbReader builds the reader from the source system.
*/
func (inb *SrcTablePipelineBuilder) buildSrcInbReader() (goetl.Processor, error) {
	queryString, err := inb.Table.GetSelectQuery()
	if err != nil {
		return nil, err
	}
	if inb.SourceDb != nil {
		// this is a legitimate source database
		reader := processors.NewSQLReader(inb.SourceDb, queryString)
		reader.BatchSize = config.Config.BatchSizeReader
		return reader, nil
	} else if inb.Driver == "json" {
		reader, err := proc.NewJSONReader(inb.Address)
		if err != nil {
			return nil, err
		}
		return reader, nil
	} else if inb.Driver == "csv" {
		reader, err := proc.NewCSVReader(inb.Address)
		if err != nil {
			return nil, err
		}
		return reader, nil
	} else {
		return nil, fmt.Errorf("unsupported source type for INB reader: %s", inb.Driver)
	}

}

/*
buildSrcInbWriter builds the Processor to insert the data into the inb table
*/
func (inb *SrcTablePipelineBuilder) buildSrcInbWriter() (*processors.PostgreSQLWriter, error) {
	destinationTable := inb.getDestinationTable()
	if destinationTable == "" {
		return nil, fmt.Errorf("the destination table cannot be blank, check the configuration")
	}
	writer := processors.NewPostgreSQLWriter(inb.TargetDb, destinationTable)
	writer.BatchSize = config.Config.BatchSizeWriter
	writer.OnDupKeyUpdate = false
	return writer, nil
}

/*
Build constructs the Pipeline for a given table
*/
func (inb *SrcTablePipelineBuilder) Build() (*goetl.Pipeline, error) {
	// build Processors
	truncator, err := inb.buildSrcInbTruncator()
	if err != nil || truncator == nil {
		return nil, err
	}
	reader, err := inb.buildSrcInbReader()
	if err != nil || reader == nil {
		return nil, err
	}
	writer, err := inb.buildSrcInbWriter()
	if err != nil || writer == nil {
		return nil, err
	}

	// build stages
	truncateAndReadStage := goetl.NewPipelineStage(goetl.Do(truncator).Outputs(writer), goetl.Do(reader).Outputs(writer))
	writerStage := goetl.NewPipelineStage(goetl.Do(writer))

	layout, err := goetl.NewPipelineLayout(truncateAndReadStage, writerStage)
	if err != nil {
		logging.EtlLogger.Info("Error in layout of pipeline for: " + inb.getDestinationTable() + " " + inb.Table.SrcSys)
		logging.EtlLogger.Error(err.Error())
		return nil, err
	}

	pipeline := goetl.NewBranchingPipeline(layout)
	pipeline.Name = inb.getDestinationTable()

	return pipeline, nil
}
