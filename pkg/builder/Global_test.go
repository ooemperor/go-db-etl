package builder

import (
	"testing"
)

func TestBuilderGlobalInsert(t *testing.T) {
	schemaName := "inb"
	tableName := "TestTable"
	script, err := BuildTruncateTableSql(schemaName, tableName)

	if err != nil {
		t.Fatalf("Error on BuildTruncateTableSql: %v", err)
	}

	if script != "TRUNCATE TABLE inb.TestTable;" {
		t.Fatalf("Truncate table statement incorrect: %v", script)
	}
}

func TestBuilderGlobalInsertSchemaEmpty(t *testing.T) {
	schemaName := ""
	tableName := "TestTable"
	_, err := BuildTruncateTableSql(schemaName, tableName)

	if err == nil {
		t.Fatalf("No error has been raised for empty schema")
	}

	if err.Error() != "the schema cannot be blank" {
		t.Fatalf("incorrect error: %v", err.Error())
	}
}

func TestScriptTransactionWrapper(t *testing.T) {
	type args struct {
		query string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{name: "InbPackageBuildTest1", args: args{query: "SELECT 1;"}, want: "DO $$ BEGIN SELECT 1; END $$ ;"},
		{name: "InbPackageBuildTest2", args: args{query: "TRUNCATE TABLE public.test;"}, want: "DO $$ BEGIN TRUNCATE TABLE public.test; END $$ ;"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ScriptTransactionWrapper(tt.args.query); got != tt.want {
				t.Errorf("ScriptTransactionWrapper() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestScriptSetTableLogged(t *testing.T) {
	type args struct {
		schema    string
		tableName string
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		{name: "ScriptSetTableLoggedTest1", args: args{schema: "public", tableName: "test"}, want: "ALTER TABLE public.test SET LOGGED;", wantErr: false},
		{name: "ScriptSetTableLoggedTest1", args: args{schema: "", tableName: "test"}, want: "", wantErr: true},
		{name: "ScriptSetTableLoggedTest1", args: args{schema: "public", tableName: ""}, want: "", wantErr: true},
		{name: "ScriptSetTableLoggedTest1", args: args{schema: "", tableName: ""}, want: "", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ScriptSetTableLogged(tt.args.schema, tt.args.tableName)
			if (err != nil) != tt.wantErr {
				t.Errorf("ScriptSetTableLogged() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("ScriptSetTableLogged() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestScriptSetTableUnlogged(t *testing.T) {
	type args struct {
		schema    string
		tableName string
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		{name: "ScriptSetTableUnloggedTest1", args: args{schema: "public", tableName: "test"}, want: "ALTER TABLE public.test SET UNLOGGED;", wantErr: false},
		{name: "ScriptSetTableUnloggedTest2", args: args{schema: "", tableName: "test"}, want: "", wantErr: true},
		{name: "ScriptSetTableUnloggedTest3", args: args{schema: "public", tableName: ""}, want: "", wantErr: true},
		{name: "ScriptSetTableUnloggedTest4", args: args{schema: "", tableName: ""}, want: "", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ScriptSetTableUnlogged(tt.args.schema, tt.args.tableName)
			if (err != nil) != tt.wantErr {
				t.Errorf("ScriptSetTableLogged() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("ScriptSetTableLogged() got = %v, want %v", got, tt.want)
			}
		})
	}
}
