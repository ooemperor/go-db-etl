package srcinb

import (
	"database/sql"
	"fmt"

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
Build constructs the Pipeline for a given table
*/
func (inb *SrcTablePipelineBuilder) Build() (*goetl.Pipeline, error) {
	queryString, _ := inb.Table.GetSelectQuery()
	destinationTable := inb.Table.Name + "_" + inb.Table.SrcSys
	truncateQuery := fmt.Sprintf("TRUNCATE TABLE %s;", destinationTable)

	truncator := processors.NewSQLExecutor(inb.TargetDb, truncateQuery)

	reader := processors.NewSQLReader(inb.SourceDb, queryString)
	reader.BatchSize = config.Config.BatchSizeReader
	writer := processors.NewPostgreSQLWriter(inb.TargetDb, destinationTable)
	writer.BatchSize = config.Config.BatchSizeWriter
	writer.OnDupKeyUpdate = false

	truncateAndReadStage := goetl.NewPipelineStage(goetl.Do(truncator).Outputs(writer), goetl.Do(reader).Outputs(writer))
	writerStage := goetl.NewPipelineStage(goetl.Do(writer))

	layout, err := goetl.NewPipelineLayout(truncateAndReadStage, writerStage)
	if err != nil {
		logging.EtlLogger.Info("Error in layout of pipeline for: " + destinationTable + " " + inb.Table.SrcSys)
		logging.EtlLogger.Error(err.Error())
		return nil, err
	}

	pipeline := goetl.NewBranchingPipeline(layout)
	pipeline.Name = destinationTable

	return pipeline, nil
}
