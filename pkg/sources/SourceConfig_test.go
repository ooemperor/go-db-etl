package sources

import (
	"fmt"
	"testing"
)

/*
Basic functionality and loading tests for the SourceConfig
*/
func TestSourceConfig_General(t *testing.T) {
	var srcConfig, err = BuildSourceConfig("./test_files/src_example.json")

	t.Run("SourceConfig_Build_Test", func(t *testing.T) {
		if err != nil {
			t.Fatalf("Error on BuildSourceConfig: %v", err)
		}
	})

	t.Run("SourceConfig_SrcSysCount_Test", func(t *testing.T) {
		srcSys := srcConfig.SrcSys
		if len(srcSys) != 2 {
			t.Fatalf("length of systems host should be 2 but got %v", len(srcSys))
		}
	})

	t.Run("SourceConfig_SrcSys_Test1", func(t *testing.T) {
		sys := srcConfig.SrcSys[0]
		if sys.Name != "mssql1" {
			t.Fatalf("expected name mssql1 but got %v", sys.Name)
		}
	})

	t.Run("SourceConfig_SrcSys_Test2", func(t *testing.T) {
		sys := srcConfig.SrcSys[0]
		if len(sys.Tables) != 1 {
			fmt.Println(sys.Tables)
			t.Fatalf("length of Tables host should be 1 but got %v", len(sys.Tables))
		}
	})

	t.Run("SourceConfig_GetActiveSystems_Test", func(t *testing.T) {
		activeSys := srcConfig.GetActiveSystems()
		if len(activeSys) != 2 {
			t.Fatalf("length of ActiveSystems host should be 2 but got %v", len(activeSys))
		}
	})
}

/*
TestBuildSourceConfig tests the general build of the source config
*/
func TestBuildSourceConfig(t *testing.T) {
	type args struct {
		fileName string
	}
	tests := []struct {
		name    string
		args    args
		want    *SourceConfig
		wantErr bool
	}{
		{name: "BuildSourceConfigTest1", args: args{fileName: "./test_files/src_example.json"}, want: &SourceConfig{}, wantErr: false},
		{name: "BuildSourceConfigTest1", args: args{fileName: "./test_files/src.json"}, want: &SourceConfig{}, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := BuildSourceConfig(tt.args.fileName)
			if (err != nil) != tt.wantErr {
				t.Errorf("BuildSourceConfig() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}
