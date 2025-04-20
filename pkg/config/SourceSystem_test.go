package config

import (
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

	if srcSys.Name != "Sys1" {
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

	if srcSys.SystemType != "postgres" {
		t.Fatalf("Error on SourceSys.SystemType: %v", srcSys.SystemType)
	}

	if srcSys.Address != "127.0.0.1" {
		t.Fatalf("Error on SourceSys.Address: %v", srcSys.Address)
	}

	if srcSys.Port != 5432 {
		t.Fatalf("Error on SourceSys.Port: %v", srcSys.Port)
	}

	if len(srcSys.GetTables()) != 1 {
		t.Fatalf("Error on SourceSys.GetTables: %v", srcSys.GetTables())
	}
}
