package utils

type WhereValue any

type Where struct {
	Path      string     `json:"path" required:"true"`
	Operation string     `json:"operation" enum:"==,!=,<,<=,>,>=,array-contains,array-contains-any,in,not-in" required:"true" title:"Operation"`
	Value     WhereValue `json:"value" configurable:"true" title:"Value"`
}
