package inb

import "github.com/teambenny/goetl"

type InbSourcePackage struct {
	tablePipelines []goetl.PipelineIface
}

func (srcP InbSourcePackage) Run() error {
	for _, tablePipeline := range srcP.tablePipelines {
		tablePipeline.Run()
	}
	return nil
}
