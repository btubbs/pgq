package pgq

import (
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
)

func getTestDB() *sqlx.DB {
	db, err := sqlx.Open("postgres", "postgres://postgres@/pgq_test?sslmode=disable")
	if err != nil {
		panic(err)
	}
	if err != nil {
		panic(err)
	}
	return db
}

func cleanDB() {
	masterDB := getMasterDB()
	defer masterDB.Close()
	_, err := masterDB.Exec("DROP DATABASE IF EXISTS pgq_test;")
	if err != nil {
		panic(err)
	}
	_, err = masterDB.Exec("CREATE DATABASE pgq_test;")
	if err != nil {
		panic(err)
	}
	db := getTestDB()
	defer db.Close()
	_, err = db.Exec(createTableSQL)
	if err != nil {
		panic(err)
	}
}

// creates a random DB, runs a function with it, then destroys the random DB
func withFreshDB(testFunc func(*sqlx.DB)) {
	masterDB := getMasterDB()
	defer masterDB.Close()
	dbName := "pgq_test_" + randString(12)
	fmt.Println("dbName", dbName)
	_, err := masterDB.Exec("CREATE DATABASE " + dbName)
	if err != nil {
		panic(err)
	}
	dbURL := fmt.Sprintf("postgres://postgres@/%s?sslmode=disable", dbName)
	db, err := sqlx.Open("postgres", dbURL)
	if err != nil {
		panic(err)
	}
	defer func() {
		// close DB
		db.Close()
		// delete DB
		masterDB.Exec("DROP DATABASE " + dbName)
	}()
	_, err = db.Exec(createTableSQL)
	if err != nil {
		panic(err)
	}
	testFunc(db)
}

func getMasterDB() *sqlx.DB {
	masterDB, err := sqlx.Open("postgres", "postgres://postgres@/postgres?sslmode=disable")
	if err != nil {
		panic(err)
	}
	return masterDB
}

func TestMain(m *testing.M) {
	cleanDB()
	os.Exit(m.Run())
}

func init() {
	rand.Seed(time.Now().UnixNano())
}

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyz")

func randString(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}
