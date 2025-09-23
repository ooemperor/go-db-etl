package inbrdv

import (
	"database/sql"

	"github.com/teambenny/goetl"
)

/*
RdvPipeline is the object that holds all the information to load a single table from rdv to bdv.
*/
type RdvPipeline struct {
	Db    *sql.DB
	Table string
}

/*
Build constructs the pipeline for a given table.
*/
func (rdv *RdvPipeline) Build() (*goetl.Pipeline, error) {
	return nil, nil
}
