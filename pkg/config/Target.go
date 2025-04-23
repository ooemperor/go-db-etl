package config

/*
Target definition of the struct
*/
type Target struct {
	sys *System
}

/*
GetConnectionString adapts to the parent method of the system.
*/
func (t Target) GetConnectionString() (interface{}, interface{}) {
	return t.sys.GetConnectionString()
}
