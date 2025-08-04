package srcinb

import (
	"database/sql"
	"fmt"
	"github.com/ooemperor/go-db-etl/pkg/logging"
	"github.com/ooemperor/go-db-etl/pkg/pipeline/srcinb"
	"github.com/ooemperor/go-db-etl/pkg/sources"
	"github.com/teambenny/goetl"
)

/*
SystemPackage objects that holds all the pipelines for the INB Layer for a System.
*/
type SystemPackage struct {
	pipelines []*goetl.Pipeline
	system    *sources.SourceSystem
}

/*
Run executes the given SystemPackage
*/
func (srcP *SystemPackage) Run() error {
	if len(srcP.pipelines) == 0 {
		msg := fmt.Sprintf("no pipelines found for SystemPackage %s", srcP.system.Name)
		logging.EtlLogger.Error(msg)
		return fmt.Errorf(msg, nil)
	}

	for _, tablePipeline := range srcP.pipelines {
		tablePipeline.PrintData = true
		c := <-tablePipeline.Run()
		if c != nil {
			logging.EtlLogger.Error(c.Error(), tablePipeline.Stats())
		}
	}
	return nil
}

func (srcP *SystemPackage) Build() error {
	logging.EtlLogger.Info("Building start INB pipeline for Source System " + srcP.system.Name)
	tables, _ := srcP.system.GetActiveTables()
	connectionString, _ := srcP.system.GetConnectionString()
	db, _ := sql.Open(srcP.system.Driver, connectionString)
	targetDb, _ := sql.Open(srcP.system.Driver, "postgres://targetUsername:targetPassword@127.0.0.1:5678/INB?sslmode=disable")
	for _, table := range tables {
		pipeBuilder := srcinb.SrcTablePipelineBuilder{SourceDb: db, TargetDb: targetDb, Table: table}
		pipeLine := pipeBuilder.Build()
		srcP.pipelines = append(srcP.pipelines, pipeLine)
	}

	logging.EtlLogger.Info("Building end INB pipeline for Source System " + srcP.system.Name)
	return nil
}

func NewSystemPackage(system *sources.SourceSystem) *SystemPackage {
	return &SystemPackage{pipelines: make([]*goetl.Pipeline, 0), system: system}
}
