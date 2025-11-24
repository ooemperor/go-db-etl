package sources

import (
	"encoding/json"
	"errors"
	"fmt"
)

type SourceTable struct {
	Name      string
	SrcSys    string
	Enabled   bool
	SrcFilter string
	SrcQuery  string
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
	if st.Name == "" {
		return "", errors.New("name is required")
	}
	if st.GetSrcQuery() != "" {
		fmt.Println("jup es geit")
		return st.GetSrcQuery(), nil
	} else if st.GetSelectFilter() != "" {
		return fmt.Sprintf("SELECT * FROM %s %s;", st.Name, st.GetSelectFilter()), nil
	} else {
		return fmt.Sprintf("SELECT * FROM %s;", st.Name), nil
	}
}

func (st *SourceTable) GetSelectFilter() string {
	return st.SrcFilter
}

func (st *SourceTable) GetSrcQuery() string {
	return st.SrcQuery
}
