package inbrdv

import (
	"database/sql"
	"fmt"

	"github.com/ooemperor/go-db-etl/pkg/builder"
	"github.com/ooemperor/go-db-etl/pkg/config"
	"github.com/ooemperor/go-db-etl/pkg/logging"
	"github.com/teambenny/goetl"
	"github.com/teambenny/goetl/processors"
)

/*
RdvPipeline is the object that holds all the information to load a single table from rdv to bdv.
*/
type RdvPipeline struct {
	db    *sql.DB
	Table string
}

/*
buildTruncator constructs the processor that truncates the targetTable
*/
func (rdv *RdvPipeline) buildTruncator() (*processors.SQLExecutor, error) {
	truncateQuery, err := builder.BuildTruncateTableSql("rdv", rdv.Table)
	if err != nil {
		return nil, err
	}
	truncator := processors.NewSQLExecutor(rdv.db, truncateQuery)
	return truncator, nil
}

/*
buildInbReader constructs the processor reads from the source table
*/
func (rdv *RdvPipeline) buildInbReader() (*processors.SQLReader, error) {
	queryString, err := builder.BuildInbRdvSatCurSelect(rdv.Table)
	if err != nil {
		return nil, err
	}
	reader := processors.NewSQLReader(rdv.db, queryString)
	reader.BatchSize = config.Config.BatchSizeReader
	return reader, nil
}

/*
buildSatCurWriter constructs the processor that writes the data into the sat_cur table
*/
func (rdv *RdvPipeline) buildSatCurWriter() (*processors.PostgreSQLWriter, error) {
	satCurTableName, err := builder.GetRdvSatCurTableName(rdv.Table)
	if err != nil {
		return nil, err
	}
	writerSatCur := processors.NewPostgreSQLWriter(rdv.db, satCurTableName)
	writerSatCur.BatchSize = config.Config.BatchSizeWriter
	writerSatCur.OnDupKeyUpdate = false
	return writerSatCur, nil
}

/*
buildSatMarkDelete constructs the processor that updates the sat records as deleted
*/
func (rdv *RdvPipeline) buildSatMarkDelete() (*processors.SQLExecutor, error) {
	deleteQuery, err := builder.BuildInbRdvSatDeleteQuery(rdv.Table)
	if err != nil {
		return nil, err
	}
	markDelete := processors.NewSQLExecutor(rdv.db, deleteQuery)
	return markDelete, nil
}

/*
buildSatInsertExecutor constructs the processor that updates the sat records as deleted
*/
func (rdv *RdvPipeline) buildSatInsertExecutor() (*processors.SQLExecutor, error) {
	query, err := builder.BuildInbRdvSatInsertQuery(rdv.Table)
	if err != nil {
		return nil, err
	}
	satInserter := processors.NewSQLExecutor(rdv.db, query)
	return satInserter, nil
}

/*
Build constructs the pipeline for a given table.
*/
func (rdv *RdvPipeline) Build() (*goetl.Pipeline, error) {
	satCurTableName, _ := builder.GetRdvSatCurTableName(rdv.Table)

	// build processors
	truncator, err := rdv.buildTruncator()
	if err != nil || truncator == nil {
		return nil, err
	}
	reader, err := rdv.buildInbReader()
	if err != nil || reader == nil {
		return nil, err
	}
	writerSatCur, err := rdv.buildSatCurWriter()
	if err != nil || writerSatCur == nil {
		return nil, err
	}
	updateDeleted, err := rdv.buildSatMarkDelete()
	if err != nil || updateDeleted == nil {
		return nil, err
	}
	satInserter, err := rdv.buildSatInsertExecutor()
	if err != nil || satInserter == nil {
		return nil, err
	}

	// build stages in order of later usage
	truncateAndReadStage := goetl.NewPipelineStage(goetl.Do(truncator).Outputs(writerSatCur), goetl.Do(reader).Outputs(writerSatCur))
	writerSatCurStage := goetl.NewPipelineStage(goetl.Do(writerSatCur).Outputs(updateDeleted))
	updateSatStage := goetl.NewPipelineStage(goetl.Do(updateDeleted).Outputs(satInserter))
	insertSatStage := goetl.NewPipelineStage(goetl.Do(satInserter))

	layout, err := goetl.NewPipelineLayout(truncateAndReadStage, writerSatCurStage, updateSatStage, insertSatStage)
	if err != nil {
		logging.EtlLogger.Info("Error in layout of pipeline for: " + rdv.Table + " ")
		logging.EtlLogger.Error(err.Error())
		return nil, err
	}

	pipeline := goetl.NewBranchingPipeline(layout)
	pipeline.Name = fmt.Sprintf("%s rdv load", satCurTableName)

	return pipeline, nil
}
