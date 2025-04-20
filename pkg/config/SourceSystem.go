package config

type SourceSystem struct {
	Name       string
	Enabled    bool
	Username   string
	Password   string
	SystemType string
	Address    string
	Port       int
	tables     []*SourceTable
}

/*
GetTables return the list of srcTables associated with this sourceSystem
*/
func (sys *SourceSystem) GetTables() []*SourceTable {
	return sys.tables
}
