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

	var srcSys = srcConfig.SrcTable[0]

	if srcSys.SrcSys != "mssql1" {
		t.Fatalf("Error on SourceSys.SrcSys: %v", srcSys.SrcSys)
	}

	if srcSys.Name != "Table1" {
		t.Fatalf("Error on SourceSys.Name: %v", srcSys.Name)
	}

	if srcSys.Enabled != true {
		t.Fatalf("Error on SourceSys.Enabled: %v", srcSys.Enabled)
	}
}
