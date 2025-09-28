package srcinb

import (
	"database/sql"

	"github.com/ooemperor/go-db-etl/pkg/builder"
	"github.com/ooemperor/go-db-etl/pkg/config"
	"github.com/ooemperor/go-db-etl/pkg/logging"
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
}

/*
getDestinationTable return the destinationTableName for the inb layer
*/
func (inb *SrcTablePipelineBuilder) getDestinationTable() string {
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
func (inb *SrcTablePipelineBuilder) buildSrcInbReader() (*processors.SQLReader, error) {
	queryString, err := inb.Table.GetSelectQuery()
	if err != nil {
		return nil, err
	}
	reader := processors.NewSQLReader(inb.SourceDb, queryString)
	reader.BatchSize = config.Config.BatchSizeReader
	return reader, nil
}

/*
buildSrcInbWriter builds the Processor to insert the data into the inb table
*/
func (inb *SrcTablePipelineBuilder) buildSrcInbWriter() (*processors.PostgreSQLWriter, error) {
	destinationTable := inb.getDestinationTable()
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
