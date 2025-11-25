package srcinb

import (
	"database/sql"
	"errors"
	"fmt"
	"sync"

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
	system    *sources.System
	target    *sources.System
}

func (srcP *SystemPackage) Name() string {
	return srcP.system.Name
}

/*
Run executes the given SystemPackage
*/
func (srcP *SystemPackage) Run() error {
	logging.EtlLogger.Info(fmt.Sprintf("START %v", srcP.Name()))
	if len(srcP.pipelines) == 0 {
		msg := fmt.Sprintf("no pipelines found for SystemPackage %s", srcP.system.Name)
		logging.EtlLogger.Error(msg)
		return errors.New(msg)
	}

	var wg sync.WaitGroup
	wg.Add(len(srcP.pipelines))
	for _, tablePipeline := range srcP.pipelines {
		go func() {
			defer wg.Done()
			c := <-tablePipeline.Run()
			if c != nil {
				logging.EtlLogger.Error(c.Error(), tablePipeline.Stats())
			}
		}()
	}
	wg.Wait()
	logging.EtlLogger.Info(fmt.Sprintf("END %v", srcP.Name()))
	return nil
}

func (srcP *SystemPackage) Build() error {
	logging.EtlLogger.Info("Building start INB pipeline for Source System " + srcP.system.Name)
	tables, _ := srcP.system.GetActiveTables()
	connectionString, _ := srcP.system.GetConnectionString()
	db, err := srcP.system.GetDB()
	if err != nil {
		logging.EtlLogger.Error(err.Error(), " on ConnectionString", connectionString)
		return err
	}
	if srcP.target == nil {
		return errors.New("no target system found, check your configuration")
	}
	targetDbConnectionString, err := srcP.target.GetConnectionString()
	if err != nil {
		logging.EtlLogger.Error(err.Error())
	}
	targetDb, err := sql.Open(srcP.target.Driver, targetDbConnectionString)
	if err != nil {
		logging.EtlLogger.Error(err.Error())
	}
	for _, table := range tables {
		pipeBuilder := srcinb.SrcTablePipelineBuilder{SourceDb: db, TargetDb: targetDb, Table: table, Driver: srcP.system.Driver, Address: connectionString}
		pipeLine, _ := pipeBuilder.Build()
		srcP.pipelines = append(srcP.pipelines, pipeLine)
	}

	logging.EtlLogger.Info("Building end INB pipeline for Source System " + srcP.system.Name)
	return nil
}

func NewSystemPackage(system *sources.System, target *sources.System) *SystemPackage {
	return &SystemPackage{pipelines: make([]*goetl.Pipeline, 0), system: system, target: target}
}
