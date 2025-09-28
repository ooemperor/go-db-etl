package srcinb

import (
	"testing"

	"github.com/ooemperor/go-db-etl/pkg/sources"
)

func TestSystemPackage_Build(t *testing.T) {
	type fields struct {
		system *sources.System
		target *sources.System
	}
	srcConfig, _ := sources.BuildSourceConfig("../../sources/test_files/src_example.json")
	sys1 := srcConfig.SrcSys[0]
	tests := []struct {
		name    string
		fields  fields
		wantErr bool
	}{
		{name: "SystemPackageBuildTest1", fields: fields{system: sys1, target: nil}, wantErr: true},
		{name: "SystemPackageBuildTest2", fields: fields{system: sys1, target: sys1}, wantErr: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srcP := NewSystemPackage(tt.fields.system, tt.fields.target)
			if err := srcP.Build(); (err != nil) != tt.wantErr {
				t.Errorf("Build() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

/*
Tests the name function of the SystemPackage and the build
*/
func TestSystemPackage_Name(t *testing.T) {
	srcConfig, _ := sources.BuildSourceConfig("../../sources/test_files/src_example.json")
	sys1 := srcConfig.SrcSys[0]

	t.Run("SystemPackageNameTest1", func(t *testing.T) {
		srcP := NewSystemPackage(sys1, sys1)
		if got := srcP.Name(); got != "mssql1" {
			t.Errorf("Name() = %v, want %v", got, "mssql1")
		}
	})

}
