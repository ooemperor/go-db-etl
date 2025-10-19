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
	Db    *sql.DB
	Table string
}

/*
buildTruncator constructs the processor that truncates the targetTable in rdv
*/
func (rdv *RdvPipeline) buildTruncator() (*processors.SQLExecutor, error) {
	satCurName, _ := builder.GetRdvSatCurTableName(rdv.Table)
	truncateQuery, err := builder.BuildTruncateTableSql("rdv", satCurName)
	if err != nil {
		return nil, err
	}
	truncator := processors.NewSQLExecutor(rdv.Db, truncateQuery)
	return truncator, nil
}

/*
buildInbReader constructs the processor reads from the source table in inb
*/
func (rdv *RdvPipeline) buildInbReader() (*processors.SQLReader, error) {
	queryString, err := builder.BuildInbRdvSatCurSelect(rdv.Table)
	if err != nil {
		return nil, err
	}
	reader := processors.NewSQLReader(rdv.Db, queryString)
	reader.BatchSize = config.Config.BatchSizeReader
	return reader, nil
}

/*
buildSatCurWriter constructs the processor that writes the data into the sat_cur table
*/
func (rdv *RdvPipeline) buildSatCurWriter() (*processors.SQLExecutor, error) {
	satCurTableName, err := builder.GetRdvSatCurTableName(rdv.Table)
	if err != nil {
		return nil, err
	}
	// writerSatCur := processors.NewPostgreSQLWriter(rdv.Db, fmt.Sprintf("rdv.%v", satCurTableName))
	// writerSatCur.BatchSize = config.Config.BatchSizeWriter
	// writerSatCur.OnDupKeyUpdate = false
	queryString, err := builder.BuildInbRdvSatCurSelect(rdv.Table)
	queryString = "INSERT INTO rdv." + satCurTableName + " " + queryString
	writerSatCur := processors.NewSQLExecutor(rdv.Db, queryString)
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
	markDelete := processors.NewSQLExecutor(rdv.Db, deleteQuery)
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
	satInserter := processors.NewSQLExecutor(rdv.Db, query)
	return satInserter, nil
}

/*
buildExecuteAll stage to execute all and everything in Stage
*/
func (rdv *RdvPipeline) buildExecuteAll() (*processors.SQLExecutor, error) {
	satCurName, _ := builder.GetRdvSatCurTableName(rdv.Table)
	truncateQuery, err := builder.BuildTruncateTableSql("rdv", satCurName)
	if err != nil {
		return nil, err
	}

	satCurWriterQuery, err := builder.BuildInbRdvSatCurSelect(rdv.Table)
	satCurWriterQuery = "INSERT INTO rdv." + satCurName + " " + satCurWriterQuery

	deleteQuery, err := builder.BuildInbRdvSatDeleteQuery(rdv.Table)
	if err != nil {
		return nil, err
	}

	satWriterQuery, err := builder.BuildInbRdvSatInsertQuery(rdv.Table)
	if err != nil {
		return nil, err
	}

	truncateQuery = builder.ScriptTransactionWrapper(truncateQuery)
	satCurWriterQuery = builder.ScriptTransactionWrapper(satCurWriterQuery)
	deleteQuery = builder.ScriptTransactionWrapper(deleteQuery)
	satWriterQuery = builder.ScriptTransactionWrapper(satWriterQuery)

	fullQuery := fmt.Sprintf("%v %v %v %v", truncateQuery, satCurWriterQuery, deleteQuery, satWriterQuery)
	fullExecutor := processors.NewSQLExecutor(rdv.Db, fullQuery)
	return fullExecutor, nil
}

/*
Build constructs the pipeline for a given table.
*/
func (rdv *RdvPipeline) Build() (*goetl.Pipeline, error) {
	satCurTableName, _ := builder.GetRdvSatCurTableName(rdv.Table)

	fullExecutor, err := rdv.buildExecuteAll()
	if err != nil || fullExecutor == nil {
		return nil, err
	}
	fullStage := goetl.NewPipelineStage(goetl.Do(fullExecutor))

	layout, err := goetl.NewPipelineLayout(fullStage)
	if err != nil {
		logging.EtlLogger.Info("Error in layout of pipeline for: " + rdv.Table + " ")
		logging.EtlLogger.Error(err.Error())
		return nil, err
	}

	pipeline := goetl.NewBranchingPipeline(layout)
	pipeline.Name = fmt.Sprintf("%s rdv load", satCurTableName)

	return pipeline, nil
}
