package inb

import (
	"database/sql"
	"fmt"
	"gihub.com/ooemperor/go-db-etl/pkg/logging"
	"gihub.com/ooemperor/go-db-etl/pkg/sources"
	"github.com/teambenny/goetl"
	"github.com/teambenny/goetl/processors"
)

/*
SrcTablePipelineBuilder constructs a simple data Pipeline for loading a single table.
*/
type SrcTablePipelineBuilder struct {
	sourceDb *sql.DB
	targetDb *sql.DB
	table    *sources.SourceTable
}

/*
Build constructs the Pipeline for a given table
*/
func (inb *SrcTablePipelineBuilder) Build() *goetl.Pipeline {
	queryString, _ := inb.table.GetSelectQuery()
	destinationTable := inb.table.Name + "_" + inb.table.SrcSys
	truncateQuery := fmt.Sprintf("TRUNCATE TABLE %s;", destinationTable)

	truncator := processors.NewSQLExecutor(inb.targetDb, truncateQuery)

	reader := processors.NewSQLReader(inb.sourceDb, queryString)
	writer := processors.NewPostgreSQLWriter(inb.targetDb, destinationTable)
	writer.OnDupKeyUpdate = false

	truncateAndReadStage := goetl.NewPipelineStage(goetl.Do(truncator).Outputs(writer), goetl.Do(reader).Outputs(writer))
	writerStage := goetl.NewPipelineStage(goetl.Do(writer))

	layout, err := goetl.NewPipelineLayout(truncateAndReadStage, writerStage)
	if err != nil {
		logging.EtlLogger.Info("Error in layout of pipeline for: " + destinationTable + " " + inb.table.SrcSys)
		logging.EtlLogger.Error(err.Error())
	}

	pipeline := goetl.NewBranchingPipeline(layout)
	pipeline.Name = destinationTable

	return pipeline
}
