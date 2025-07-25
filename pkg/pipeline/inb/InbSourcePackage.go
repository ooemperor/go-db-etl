package inb

import (
	"github.com/teambenny/goetl"
	"go-db-etl/pkg/logging"
)

type SourcePackage struct {
	tablePipelines []*goetl.Pipeline
}

func (srcP SourcePackage) Run() error {
	for _, tablePipeline := range srcP.tablePipelines {
		tablePipeline.PrintData = true
		c := <-tablePipeline.Run()
		if c != nil {
			logging.EtlLogger.Info(c.Error(), tablePipeline.Stats())
		}

	}
	return nil
}
