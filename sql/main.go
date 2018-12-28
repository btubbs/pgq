package main

import (
	"io/ioutil"
	"os"
	"strconv"
	"text/template"
)

// A tiny utility for rebuilding the create_table.go file from create_table.sql.  This should be run
// from the project root, like "go run sql/main.go".
func main() {
	t := template.Must(template.New("createTable").Parse(raw))
	outFile, err := os.Create("create_table.go")
	if err != nil {
		panic(err)
	}
	sql, err := ioutil.ReadFile("sql/create_table.sql")
	if err != nil {
		panic(err)
	}
	err = t.Execute(outFile, map[string]string{"sql": strconv.Quote(string(sql))})
	if err != nil {
		panic(err)
	}
}

const raw = `package pgq

// DO NOT EDIT.  Use "go generate" to regenerate this file from create_table.sql.
//go:generate go run sql/main.go

const createTableSQL = {{.sql}}
`
