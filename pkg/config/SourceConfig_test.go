package config

import (
	"testing"
)

/*
TestSourceConfig tests the setup of the sourceConfig
*/
func TestSourceConfig(t *testing.T) {
	var srcConfig, err = BuildSourceConfig("./test_files/src_example.json")

	if err != nil {
		t.Fatalf("Error on BuildSourceConfig: %v", err)
	}

	var srcTables = srcConfig.SrcTable
	var srcSys = srcConfig.SrcSys

	if len(srcTables) != 2 {
		t.Fatalf("length of tables host should be 2 but got %v", len(srcTables))
	}

	if len(srcSys) != 2 {
		t.Fatalf("length of systems host should be 2 but got %v", len(srcSys))
	}
}
