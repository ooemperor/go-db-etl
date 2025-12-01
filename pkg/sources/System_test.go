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

	t.Run("SourceSystemNameTest", func(t *testing.T) {
		if srcSys.Name != "mssql1" {
			t.Fatalf("Error on SourceSys.Name: %v", srcSys.Name)
		}
	})

	t.Run("SourceSystemEnabledTest", func(t *testing.T) {
		if srcSys.Enabled != true {
			t.Fatalf("Error on SourceSys.Enabled: %v", srcSys.Enabled)
		}
	})

	t.Run("SourceSystemUsernameTest", func(t *testing.T) {
		if srcSys.Username != "username1" {
			t.Fatalf("Error on SourceSys.Username: %v", srcSys.Username)
		}
	})

	t.Run("SourceSystemPasswordTest", func(t *testing.T) {
		if srcSys.Password != "password1" {
			t.Fatalf("Error on SourceSys.Password: %v", srcSys.Password)
		}
	})

	t.Run("SourceSystemDriverTest", func(t *testing.T) {
		if srcSys.Driver != "postgres" {
			t.Fatalf("Error on SourceSys.Driver: %v", srcSys.Driver)
		}
	})

	t.Run("SourceSystemAddressTest", func(t *testing.T) {
		if srcSys.Address != "127.0.0.1" {
			t.Fatalf("Error on SourceSys.Address: %v", srcSys.Address)
		}
	})

	t.Run("SourceSystemPortTest", func(t *testing.T) {
		if srcSys.Port != 5432 {
			t.Fatalf("Error on SourceSys.Port: %v", srcSys.Port)
		}
	})

	t.Run("SourceSystemDatabaseTest", func(t *testing.T) {
		if srcSys.Database != "db" {
			t.Fatalf("Error on SourceSys.Database: %v", srcSys.Database)
		}
	})

	t.Run("SourceSystemAllTablesTest", func(t *testing.T) {
		allTables, _ := srcSys.GetAllTables()
		if len(allTables) != 1 {
			t.Fatalf("SourceSys.GetAllTables wrong amount of tables: recevied: %v expected: %v", len(allTables), 1)
		}
	})

	t.Run("SourceSystemActiveTablesTest1", func(t *testing.T) {
		activeTables, _ := srcSys.GetActiveTables()
		if len(activeTables) != 1 {
			t.Fatalf("SourceSys.GetActiveTables wrong amount of tables: recevied: %v expected: %v", len(activeTables), 1)
		}
	})

	var srcSys2 = srcConfig.SrcSys[1]
	t.Run("SourceSystemActiveTablesTest2", func(t *testing.T) {
		activeTables, _ := srcSys2.GetActiveTables()
		if len(activeTables) != 0 {
			t.Fatalf("SourceSys.GetActiveTables wrong amount of tables: recevied: %v expected: %v", len(activeTables), 0)
		}
	})

}

/*
TestSourceSystemQueryString tests the retreival of the System Connection String
*/
func TestSourceSystemConnectionString(t *testing.T) {
	t.Run("MsSqlConnectionStringTest", func(t *testing.T) {
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

		if selectSTring != "sqlserver://usernamemssql:passwordmssql@127.0.0.1:1433?database=exampleDB&TrustServerCertificate=true&Trusted_Connection=true" {
			t.Fatalf("Error on GetConnectionString: %v", selectSTring)
		}
	})

	t.Run("PostgresConnectionStringTest", func(t *testing.T) {
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
	})

	t.Run("INvaldiDriverConnectionStringTest", func(t *testing.T) {
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
	})
}
