package sources

import (
	"fmt"
)

type SourceTable struct {
	Name      string
	SrcSys    string
	Enabled   bool
	srcFilter string
	srcQuery  string
}

/*
GetSelectQuery assembles the selectQuery for the given table.
*/
func (st *SourceTable) GetSelectQuery() (string, error) {
	if st.srcQuery != "" {
		return st.srcQuery, nil
	} else if st.srcFilter != "" {
		return fmt.Sprintf("SELECT * FROM %s %s;", st.Name, st.srcFilter), nil
	} else {
		return fmt.Sprintf("SELECT * FROM %s;", st.Name), nil
	}
}

func (st *SourceTable) GetSelectFilter() (string, error) {
	return st.srcFilter, nil
}

func (st *SourceTable) GetSrcQuery() (string, error) {
	return st.srcQuery, nil
}
