package main

import (
	"database/sql"
	"fmt"
	"os"
	"strconv"

	cli "gopkg.in/urfave/cli.v1"

	"github.com/btubbs/pgq"
	_ "github.com/lib/pq"
)

func main() {
	cliApp := cli.NewApp()
	cliApp.Name = "pgq_example"
	cliApp.Usage = "Example job publisher and worker"

	// This assumes that
	// 1. you have Postgres running locally,
	// 2. listening on a Unix socket,
	// 3. not requiring a password for the user "postgres"
	// 4. you have created the "pgq_test" database
	// 5. you have created the pgq_jobs table in that database, as shown in the create_table.sql file in the pgq repo.
	db, err := sql.Open("postgres", "postgres://postgres@/pgq_test?sslmode=disable")
	if err != nil {
		panic(err)
	}
	cliApp.Commands = []cli.Command{
		{
			Name: "enqueue",
			Action: func(c *cli.Context) error {
				return publishJobs(db)
			},
		},
		{
			Name: "run",
			Action: func(c *cli.Context) error {
				return run(db)
			},
		},
	}
	err = cliApp.Run(os.Args)
	if err != nil {
		fmt.Println(err)
		panic(err)
	}
}

func publishJobs(db *sql.DB) error {
	worker := pgq.NewWorker(db)
	_, err := worker.EnqueueJob("sayHello", []byte("Brent"))
	if err != nil {
		return err
	}
	// Register a couple more.  don't ignore errors like this in real code.
	worker.EnqueueJob("sayHello", []byte("World"))
	worker.EnqueueJob("addOne", []byte("7"))
	return nil
}

func run(db *sql.DB) error {
	worker := pgq.NewWorker(db)

	// register handlers for all the job types we care about.
	err := worker.RegisterQueue("sayHello", func(data []byte) error {
		_, err := fmt.Printf("Hello %s!\n", string(data))
		return err
	})
	if err != nil {
		return err
	}

	worker.RegisterQueue("addOne", func(data []byte) error {
		// turn our bytes into a number
		num, err := strconv.Atoi(string(data))
		if err != nil {
			return err
		}
		// add one to it
		_, err = fmt.Printf("%s plus 1 is %d\n", string(data), num+1)
		return err
	})
	return worker.Run()
}
