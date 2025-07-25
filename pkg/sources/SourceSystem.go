package sources

import (
	_ "github.com/lib/pq"
	_ "github.com/microsoft/go-mssqldb"
)

/*
SourceSystem object that wraps the System Object
*/
type SourceSystem struct {
	*System
}
