package config

import (
	"reflect"
	"testing"

	"github.com/joho/godotenv"
)

/*
TestCases for the config
*/
func TestConfiguration_Init(t *testing.T) {
	config := Configuration{timeout: 10, Name: "go-db-etl", BatchSizeReader: 10000, BatchSizeWriter: 1000, EtlLogLevel: 3, RunSrcInb: true, RunInbRdv: true}
	tests := []struct {
		name    string
		want    *Configuration
		wantErr bool
	}{
		{name: "ConfigTest1", want: &config, wantErr: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conf := &Configuration{}
			_ = godotenv.Load("../../.env")
			got, err := conf.Init()
			if (err != nil) != tt.wantErr {
				t.Errorf("Init() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Init() got = %v, want %v", got, tt.want)
			}
		})
	}
}
