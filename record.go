package gorient

import (
	"fmt"
)

type Rid struct {
	Cluster int16
	Position int64
}

type ResultSet struct {
	Records []Record
	Prefetch map[Rid]Record
}

type Record struct {
	Version int32
	Value interface{}
}

type Document struct {
	Class string
	Fields map[string]interface{}
}

func (d *Document) String() string {
	if len(d.Class) > 0 {
		return fmt.Sprintf("%s(%v)", d.Class, d.Fields)
	}
	return fmt.Sprintf("(%v)", d.Fields)
}
