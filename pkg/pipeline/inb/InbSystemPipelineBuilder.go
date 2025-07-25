package inb

import (
	"database/sql"
	"github.com/teambenny/goetl"
	"go-db-etl/pkg/logging"
	"go-db-etl/pkg/pipeline"
	"go-db-etl/pkg/sources"
)

type InbSystemPipelineBuilder struct {
	pipelines []goetl.PipelineIface
	system    *sources.SourceSystem
}

func (inb *InbSystemPipelineBuilder) Build() (pipeline.IPackage, error) {
	logging.EtlLogger.Info("Building start INB pipeline for Source System " + inb.system.Name)
	tables, _ := inb.system.GetActiveTables()
	connectionString, _ := inb.system.GetConnectionString()
	db, _ := sql.Open(inb.system.Driver, connectionString)
	for _, table := range tables {
		pipeBuilder := InbTablePipelineBuilder{db: db, table: table}
		pipeLine, err := pipeBuilder.Build()
		if err != nil {
			logging.EtlLogger.Error("Error building INB pipeline " + table.Name + ": " + err.Error())
		}
		inb.pipelines = append(inb.pipelines, pipeLine)
	}

	logging.EtlLogger.Info("Building end INB pipeline for Source System " + inb.system.Name)
	return InbSourcePackage{tablePipelines: inb.pipelines}, nil
}

func NewInbSystemWrapper(system *sources.SourceSystem) *InbSystemPipelineBuilder {
	return &InbSystemPipelineBuilder{pipelines: make([]goetl.PipelineIface, 0), system: system}
}
