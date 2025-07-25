package pipeline

import (
	"database/sql"
	"github.com/teambenny/goetl"
	"github.com/teambenny/goetl/processors"
	"go-db-etl/pkg/sources"
)

type InbPipeline struct {
	db    *sql.DB
	table sources.SourceTable
}

func (inb *InbPipeline) Build() error {
	queryString, _ := inb.table.GetSelectQuery()
	loader1 := processors.NewSQLReader(inb.db, queryString)
	inserter1 := processors.NewPostgreSQLWriter(inb.db, inb.table.Name+inb.table.SrcSys)

	stage1 := goetl.NewPipelineStage(goetl.Do(loader1).Outputs(inserter1))
	stage2 := goetl.NewPipelineStage(goetl.Do(inserter1))

	layout, err := goetl.NewPipelineLayout(
		stage1,
		stage2,
	)

	if err != nil {
		return err
	}

	pipeline := goetl.NewBranchingPipeline(layout)
	pipeline.Run()
	return nil
}
