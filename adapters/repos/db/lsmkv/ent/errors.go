package ent

import "fmt"

var (
	NotFound = fmt.Errorf("not found")
	Deleted  = fmt.Errorf("deleted")
)
