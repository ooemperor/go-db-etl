package config

import (
	"testing"
)

/*
TestSourceTableLoad tests the setup of the sourceConfig
*/
func TestSourceTableLoad(t *testing.T) {
	var srcConfig, err = BuildSourceConfig("./test_files/src_example.json")

	if err != nil {
		t.Fatalf("Error on BuildSourceConfig: %v", err)
	}

	var srcTable = srcConfig.SrcTable[0]

	if srcTable.SrcSys != "mssql1" {
		t.Fatalf("Error on SourceSys.SrcSys: %v", srcTable.SrcSys)
	}

	if srcTable.Name != "Table1" {
		t.Fatalf("Error on SourceSys.Name: %v", srcTable.Name)
	}

	if srcTable.Enabled != true {
		t.Fatalf("Error on SourceSys.Enabled: %v", srcTable.Enabled)
	}
}

func TestSourceTableLoadFromFile(t *testing.T) {
	var srcConfig, err = BuildSourceConfig("./test_files/src_example.json")

	if err != nil {
		t.Fatalf("Error on BuildSourceConfig: %v", err)
	}

	var _ = srcConfig.SrcTable[0]
}
