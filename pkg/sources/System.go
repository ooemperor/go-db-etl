package sources

import (
	"errors"
	"fmt"

	_ "github.com/jackc/pgx"
	_ "github.com/lib/pq"
	_ "github.com/microsoft/go-mssqldb"
)

type System struct {
	Name     string
	Enabled  bool
	Username string
	Password string
	Driver   string
	Address  string
	Port     int
	Database string
	Tables   []*SourceTable
}

/*
GetAllTables return the list of srcTables associated with this sourceSystem
*/
func (sys *System) GetAllTables() ([]*SourceTable, error) {
	return sys.Tables, nil
}

/*
GetActiveTables return the list of srcTables associated with this sourceSystem
*/
func (sys *System) GetActiveTables() ([]*SourceTable, error) {
	var outputTables []*SourceTable
	for _, t := range sys.Tables {
		if t.Enabled {
			outputTables = append(outputTables, t)
		}
	}
	return outputTables, nil
}

/*
GetConnectionString assembles the connection string for the given Source system
*/
func (sys *System) GetConnectionString() (string, error) {
	switch sys.Driver {
	case "mssql":
		// sqlserver://username:password@host/instance?param1=value&param2=value
		return fmt.Sprintf("sqlserver://%s:%s@%s:%d?database=%s", sys.Username, sys.Password, sys.Address, sys.Port, sys.Database), nil

	case "postgres":
		// "postgres://username:password@localhost:5432/database_name"
		return fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=disable", sys.Username, sys.Password, sys.Address, sys.Port, sys.Database), nil

	default:
		return "", errors.New("unsupported driver")
	}

}
