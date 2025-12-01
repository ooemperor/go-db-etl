package inbrdv

import (
	"database/sql"
	"errors"
	"fmt"
	"sync"

	"github.com/ooemperor/go-db-etl/pkg/logging"
	"github.com/ooemperor/go-db-etl/pkg/pipeline/inbrdv"
	"github.com/ooemperor/go-db-etl/pkg/sources"
	"github.com/teambenny/goetl"
)

/*
InbPackage objects that holds all the pipelines for the INB Layer for a System.
*/
type InbPackage struct {
	pipelines []*goetl.Pipeline
	system    *sources.System
	target    *sources.System
}

func (inbP *InbPackage) Name() string {
	return fmt.Sprintf("%v_inbrdv", inbP.system.Name)
}

/*
Run executes the given SystemPackage
*/
func (inbP *InbPackage) Run() error {
	logging.EtlLogger.Info(fmt.Sprintf("START %v", inbP.Name()))
	if len(inbP.pipelines) == 0 {
		msg := fmt.Sprintf("no pipelines found for InbPackage %s", inbP.Name())
		logging.EtlLogger.Error(msg)
		return errors.New(msg)
	}

	var wg sync.WaitGroup
	wg.Add(len(inbP.pipelines))
	for _, tablePipeline := range inbP.pipelines {
		go func() {
			defer wg.Done()
			c := <-tablePipeline.Run()
			if c != nil {
				logging.EtlLogger.Error(c.Error(), tablePipeline.Stats())
			}
		}()
	}
	wg.Wait()
	logging.EtlLogger.Info(fmt.Sprintf("END %v", inbP.Name()))
	return nil
}

func (inbP *InbPackage) Build() error {
	logging.EtlLogger.Info("Building start INB pipeline for Source System " + inbP.system.Name)
	tables, _ := inbP.system.GetActiveTables()
	if inbP.target == nil {
		return errors.New("no target system found, check your configuration")
	}
	connectionString, err := inbP.target.GetConnectionString()
	if err != nil {
		logging.EtlLogger.Error(err.Error())
	}
	db, err := sql.Open(inbP.target.Driver, connectionString)
	if err != nil {
		logging.EtlLogger.Error(err.Error(), " on ConnectionString", connectionString)
		return err
	}

	if inbP.target == nil {
		return errors.New("no target system found, check your configuration")
	}
	for _, table := range tables {
		tableName := fmt.Sprintf("%v_%v", table.Name, table.SrcSys)
		pipeBuilder := inbrdv.RdvPipeline{Db: db, Table: tableName}
		pipeLine, _ := pipeBuilder.Build()
		inbP.pipelines = append(inbP.pipelines, pipeLine)
	}

	logging.EtlLogger.Info("Building end INB pipeline for Source System " + inbP.system.Name)
	return nil
}

func NewInbRdvPackage(system *sources.System, target *sources.System) *InbPackage {
	return &InbPackage{pipelines: make([]*goetl.Pipeline, 0), system: system, target: target}
}
