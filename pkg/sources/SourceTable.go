package sources

import (
	"encoding/json"
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
UnmarshalJson implements the interface to allow proper deserializing of the json obejct
*/
func (st *SourceTable) UnmarshalJson(b []byte) error {
	var jsonString string
	if err := json.Unmarshal(b, &jsonString); err != nil {
		return err // Means the string was invalid
	}
	type ST SourceTable // A new type that doesn't have UnmarshalJSON method
	return json.Unmarshal([]byte(jsonString), (*ST)(st))
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
