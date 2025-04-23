package config

import "testing"

func TestTargetPostgresQueryString(t *testing.T) {
	s := System{
		Name:     "postgresTarget",
		Database: "INB",
		Username: "targetUsername",
		Password: "targetPassword",
		Driver:   "postgres",
		Address:  "127.0.0.1",
		Port:     5432,
	}

	target := Target{sys: &s}

	selectSTring, err := target.GetConnectionString()
	if err != nil {
		t.Fatalf("Error on GetConnectionString: %v", err)
	}

	if selectSTring != "postgres://targetUsername:targetPassword@127.0.0.1:5432/INB?sslmode=disable" {
		t.Fatalf("Error on GetConnectionString: %v", selectSTring)
	}
}
