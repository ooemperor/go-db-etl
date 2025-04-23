package sources

import (
	"errors"
	"testing"
)

/*
TestSourceTableLoad tests the setup of the sourceConfig
*/
func TestSourceSystemLoad(t *testing.T) {
	var srcConfig, err = BuildSourceConfig("./test_files/src_example.json")

	if err != nil {
		t.Fatalf("Error on BuildSourceConfig: %v", err)
	}

	var srcSys = srcConfig.SrcSys[0]

	if srcSys.Name != "mssql1" {
		t.Fatalf("Error on SourceSys.Name: %v", srcSys.Name)
	}

	if srcSys.Enabled != true {
		t.Fatalf("Error on SourceSys.Enabled: %v", srcSys.Enabled)
	}

	if srcSys.Username != "username1" {
		t.Fatalf("Error on SourceSys.Username: %v", srcSys.Username)
	}

	if srcSys.Password != "password1" {
		t.Fatalf("Error on SourceSys.Password: %v", srcSys.Password)
	}

	if srcSys.Driver != "postgres" {
		t.Fatalf("Error on SourceSys.Driver: %v", srcSys.Driver)
	}

	if srcSys.Address != "127.0.0.1" {
		t.Fatalf("Error on SourceSys.Address: %v", srcSys.Address)
	}

	if srcSys.Port != 5432 {
		t.Fatalf("Error on SourceSys.Port: %v", srcSys.Port)
	}
	allTables, _ := srcSys.GetAllTables()
	if len(allTables) != 1 {
		t.Fatalf("Error on SourceSys.GetTables: %v", len(allTables))
	}
}

func TestSourceSystemMsSqlQueryString(t *testing.T) {
	srcSys := System{
		Name:     "mssql",
		Database: "exampleDB",
		Enabled:  true,
		Username: "usernamemssql",
		Password: "passwordmssql",
		Driver:   "mssql",
		Address:  "127.0.0.1",
		Port:     1433,
	}

	selectSTring, err := srcSys.GetConnectionString()
	if err != nil {
		t.Fatalf("Error on GetConnectionString: %v", err)
	}

	if selectSTring != "sqlserver://usernamemssql:passwordmssql@127.0.0.1:1433?database=exampleDB" {
		t.Fatalf("Error on GetConnectionString: %v", selectSTring)
	}
}

func TestSourceSystemPostgresQueryString(t *testing.T) {
	srcSys := System{
		Name:     "postgres1",
		Database: "exampleDB",
		Enabled:  true,
		Username: "usernamepostgres",
		Password: "passwordpostgres",
		Driver:   "postgres",
		Address:  "127.0.0.1",
		Port:     5432,
	}

	selectSTring, err := srcSys.GetConnectionString()
	if err != nil {
		t.Fatalf("Error on GetConnectionString: %v", err)
	}

	if selectSTring != "postgres://usernamepostgres:passwordpostgres@127.0.0.1:5432/exampleDB?sslmode=disable" {
		t.Fatalf("Error on GetConnectionString: %v", selectSTring)
	}
}

func TestSourceSystemInvalidDriver(t *testing.T) {
	srcSys := System{
		Name:     "postgres1",
		Database: "exampleDB",
		Enabled:  true,
		Username: "usernamepostgres",
		Password: "passwordpostgres",
		Driver:   "garbageDriver",
		Address:  "127.0.0.1",
		Port:     5432,
	}

	connString, err := srcSys.GetConnectionString()

	if connString != "" {
		t.Fatalf("Connection string is not expected but got non empty string")
	}

	if err == nil {
		t.Fatalf("No error raised on GetConnectionString, but was expected")
	}
	expectedError := errors.New("unsupported driver")
	if err.Error() != expectedError.Error() {
		t.Fatalf("Error raised on GetConnectionString, but was expected: %v, got: %v", expectedError, err)
	}

}
