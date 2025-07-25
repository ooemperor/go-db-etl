package inb

import (
	"database/sql"
	"github.com/teambenny/goetl"
	"go-db-etl/pkg/logging"
	"go-db-etl/pkg/pipeline"
	"go-db-etl/pkg/sources"
)

type SystemPipelineBuilder struct {
	pipelines []*goetl.Pipeline
	system    *sources.SourceSystem
}

func (inb *SystemPipelineBuilder) Build() (pipeline.IPackage, error) {
	logging.EtlLogger.Info("Building start INB pipeline for Source System " + inb.system.Name)
	tables, _ := inb.system.GetActiveTables()
	connectionString, _ := inb.system.GetConnectionString()
	db, _ := sql.Open(inb.system.Driver, connectionString)
	targetDb, _ := sql.Open(inb.system.Driver, "postgres://targetUsername:targetPassword@127.0.0.1:5678/INB?sslmode=disable")
	for _, table := range tables {
		pipeBuilder := TablePipelineBuilder{sourceDb: db, targetDb: targetDb, table: table}
		pipeLine := pipeBuilder.Build()
		inb.pipelines = append(inb.pipelines, pipeLine)
	}

	logging.EtlLogger.Info("Building end INB pipeline for Source System " + inb.system.Name)
	return SourcePackage{tablePipelines: inb.pipelines}, nil
}

func NewInbSystemWrapper(system *sources.SourceSystem) *SystemPipelineBuilder {
	return &SystemPipelineBuilder{pipelines: make([]*goetl.Pipeline, 0), system: system}
}
