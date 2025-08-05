package sources

import (
	"fmt"
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

	//var srcTables = srcConfig.SrcTable
	var srcSys = srcConfig.SrcSys
	fmt.Println(srcSys)

	if len(srcSys) != 2 {
		t.Fatalf("length of systems host should be 2 but got %v", len(srcSys))
	}
}

/*
TestSourceConfig tests the setup of the sourceConfig
*/
func TestSourceConfigTables(t *testing.T) {
	var srcConfig, err = BuildSourceConfig("./test_files/src_example.json")

	if err != nil {
		t.Fatalf("Error on BuildSourceConfig: %v", err)
	}

	var srcSys = srcConfig.SrcSys

	sys := srcSys[0]

	if sys.Name != "mssql1" {
		t.Fatalf("expected name mssql1 but got %v", sys.Name)
	}

	if len(sys.Tables) != 1 {
		fmt.Println(sys.Tables)
		t.Fatalf("length of Tables host should be 1 but got %v", len(sys.Tables))
	}
}
