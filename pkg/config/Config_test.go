package config

import (
	"reflect"
	"testing"
)

/*
TestCases for the config
*/
func TestConfiguration_Init(t *testing.T) {
	config := Configuration{timeout: 10, Name: "go-db-etl", BatchSizeReader: 10000, BatchSizeWriter: 1000}
	type args struct {
		envFile string
	}
	tests := []struct {
		name    string
		args    args
		want    *Configuration
		wantErr bool
	}{
		{name: "ConfigTest1", args: args{envFile: "../../.env"}, want: &config, wantErr: false},
		{name: "ConfigTest1", args: args{envFile: ".nonExistant"}, want: &Configuration{}, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conf := &Configuration{}
			got, err := conf.Init(tt.args.envFile)
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
