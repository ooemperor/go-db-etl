package inb

import (
	"database/sql"
	"fmt"
	"github.com/teambenny/goetl"
	"github.com/teambenny/goetl/processors"
	"go-db-etl/pkg/sources"
)

type TablePipelineBuilder struct {
	sourceDb *sql.DB
	targetDb *sql.DB
	table    *sources.SourceTable
}

func (inb *TablePipelineBuilder) Build() *goetl.Pipeline {
	queryString, _ := inb.table.GetSelectQuery()
	fmt.Println(queryString)
	destinationTable := inb.table.Name + "_" + inb.table.SrcSys
	fmt.Println(destinationTable)
	reader := processors.NewSQLReader(inb.sourceDb, queryString)
	writer := processors.NewPostgreSQLWriter(inb.targetDb, destinationTable)

	pipeline := goetl.NewPipeline(reader, writer)
	return pipeline
}
