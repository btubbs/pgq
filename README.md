# pgq [![Build Status](https://travis-ci.org/btubbs/pgq.svg?branch=master)](https://travis-ci.org/btubbs/pgq) [![Coverage Status](https://coveralls.io/repos/github/btubbs/pgq/badge.svg?branch=master)](https://coveralls.io/github/btubbs/pgq?branch=master)

pgq is a Go library for job queues that use Postgres for persistence.  It builds on the [SKIP
LOCKED](https://blog.2ndquadrant.com/what-is-select-skip-locked-for-in-postgresql-9-5/)
functionality added in Postgres 9.5, which makes its queries very simple and readable.

## Installation and Setup

You can install pgq with `go get`:

    go get github.com/btubbs/pgq

Before you can enqueue or process jobs with pgq, your Postgres database will need the table and
index defined in `sql/create_table.sql`.  You can paste this right into `psql` command line while
connected to your database, or paste it into the migration tool of your choice.  (I'm partial to
[Pomegranate](https://github.com/btubbs/pomegranate).)

## Usage

There are a few steps to using pgq:

1. Define job handler functions.
2. Instantiate a new JobRunner.
3. Register your handler functions with the JobRunner.
4. Start the JobRunner.
5. In another process, enqueue jobs with a JobRunner.

Each of these steps will be explained in more detail below.

### Defining Job Handlers

While pgq will take care of pulling jobs off the queue, it's up to you to tell it exactly what each
job does.  This is done by defining and registering handler functions.  A job handler func takes one
`[]byte` argument, and returns either an error or `nil`.  Because job payloads are sent as bytes,
you can put whatever data you need in there.  It may be plain text, a URL, JSON, or a binary
encoding like protobuf.

Here's a simple example:

```go
func SayHello(data []byte) error {
  name := string(data)
  fmt.Println("Hello " + name)
}
```

That example might be too simple for a real app though.  You'll probably want your jobs to have
access to some resources, like config or a database.  In that case you can use either struct methods or
closures to inject those resources.  Here's an example of a struct method job handler:

```go
type MyJobWorker struct {
  db *sql.DB
}

func (worker *MyJobWorker) SayGoodbye(data []byte) error {
  // this is a silly example of a DB query, but illustrates the point.
  var message string
  err := worker.db.QueryRow(`SELECT 'Goodbye ' || $1`, string(data)).Scan(&message)
  if err != nil {
    return err
  }
  fmt.Println(message)
}
```

Here's the equivalent using a closure:

```go
package main

func main() {
  db, _ := sql.Open("postgres", "postgres://postgres@/my_database?sslmode=disable")

  myJobHandler := buildGoodbyeHandler(db)
}

func buildGoodbyeHandler(db *sql.DB) func([]byte) error {
	return func(data []byte) error {
		// we have access to db here because this function is declared in the same scope that db lives in
		var message string
		err := db.QueryRow(`SELECT 'Goodbye ' || $1`, string(data)).Scan(&message)
		if err != nil {
			return err
		}
		fmt.Println(message)
	}
}
```

(That example, like several below omits some error handling code.  Don't do that in real life.  It's
only done here so you can see details about using pgq without getting lost in error handling noise.)

### Processing Jobs with the JobRunner 

Once you've defined a handler function, you need to register it with an instance of `pgq.JobRunner`.
You can get one of those by passing your Postgres `*sql.DB` instance into `pgq.NewJobRunner`.

```go
db, _ := sql.Open("postgres", "postgres://postgres@/my_database?sslmode=disable")
runner := pgq.NewJobRunner(db)
```

Once you have a runner, you can register your job handler functions with it using `RegisterQueue`
and passing two arguments: the queue name (which can be any string you like), and your handler
function.

```go
err := runner.RegisterQueue("hello", SayHello)
```

If you're using struct methods as job handler functions, you need to instantiate your struct and
then register its methods.  This example uses the `MyJobWorker` struct shown above.

```go
db, _ := sql.Open("postgres", "postgres://postgres@/my_database?sslmode=disable")
worker := &MyJobWorker{db: db}
err := runner.RegisterQueue("goodbye", worker.SayGoodbye)
```

If you attempt to register the same queue name more than once on the same runner, you'll get an
error.

Once your handler functions have been registered, you start the job runner by calling its `Run`
method.  This call will query for uncompleted jobs and call the appropriate handler function for each
of them.  This will loop and perform jobs forever, unless it encounters an unrecoverable error or is
forcibly killed.

```go
err := runner.Run()
```

The runner will only query for jobs that have a queue name that matches one of its registered
handlers.  If there's a job in the queue with an unregistered queue name, it will be ignored.  You
can use this feature to start separate processes for handling different job types.  This is useful
if some queues need to be scaled up to more worker processes than others.

### Putting Jobs on the Queue

To enqueue jobs you need to have a JobRunner instance and then call its `EnqueueJob` method, which
takes a queue name, a `[]byte` payload, and some optional arguments we'll cover later.  `EnqueueJob`
will return an integer job ID (only useful for logging/debugging), and possibly an error.

```go
db, _ := sql.Open("postgres", "postgres://postgres@/my_database?sslmode=disable")
runner := pgq.NewJobRunner(db)
jobID, err := runner.EnqueueJob("hello", []byte("Brent"))
```

If you're using the same Postgres database for both pgq and your own application tables, you might
want to have the enqueueing of jobs happen in the same database transaction where you're doing other
database calls.  In that case, you can use the runner's `EnqueueJobInTx` method, which lets you pass
in your own transaction object.  It's up to you to ensure that the transaction is properly committed
or rolled back. Example:

```go
db, _ := sql.Open("postgres", "postgres://postgres@/my_database?sslmode=disable")
runner := pgq.NewJobRunner(db)

// later on in some other part of your app...
tx, _ := db.Begin()

// we use the same transaction for creating the user and enqueueing the welcome email
// so both will succeed or fail together, and we avoid sending a welcome email to a user 
// whose account never actually got created because the tx got rolled back.
createUser(tx, someUserData)
jobID, err := runner.EnqueueJobInTx(tx, "sendWelcomeEmail", someWelcomeEmailData)

if thereWasAnErrorSomewhereElse {
  tx.Rollback()
} else {
  tx.Commit()
}

```

### Additional Options

There are some additional things you can configure, both at the per-runner level and the per-job
level.

#### Runner Options

These options are passed as additional arguments when calling `pgq.NewJobRunner`, using the [functional
options](https://dave.cheney.net/2014/10/17/functional-options-for-friendly-apis) pattern.

##### JobPollingInterval

After a runner completes a job, it immediately queries for another one.  If there are no jobs
waiting in a queue, it will sleep for a few seconds before querying again.  By default that's 10
seconds.  You can change that default by passing in the `JobPollingInterval` option with a
time.Duration to `NewJobRunner`.  In this example we increase it to 30 seconds.

```go
runner, err := pgq.NewJobRunner(
  db,
  pgq.JobPollingInterval(time.Second * 30),
)
```

##### PreserveCompletedJobs

By default, jobs are deleted from the `pgq_jobs` table after being performed.  If you would prefer
to leave them in the table for analytics or debugging purposes, you can pass the
`PreserveCompletedJobs` option to `pgq.NewJobRunner`:

```go
runner, err := pgq.NewJobRunner(
  db,
  pgq.PreserveCompletedJobs,
)
```

#### Job Options

##### After

If you want a job to be processed at some time in the future instead of immediately, you can pass
the `pgq.After` option to the runner's `EnqueueJob` method.  This option takes a `time.Time`.  This
example sets a job to be run 24 hours in the future.

```go
jobID, err := runner.EnqueueJob(
  "sendWelcomeEmail",
  someWelcomeData,
  pgq.After(time.Now().Add(time.Minute * 60 * 24)),
)
```

##### RetryWaits

Sometimes jobs fail through no fault of their own.  It could be because of downtime in a database,
or a third party API, or a flaky network.  It can be useful to retry jobs automatically when such
things happen.

But you don't want to retry immediately.  A problem that's happening now will probably still be
happening one second from now.

For this reason pgq supports setting a per-job option for the number of retries to perform and how
long to wait before each.  If a job returns an error, and it's configured to allow one or more
retries, then it will be run again.

By default pgq does three retries, with one minute, ten minute, and 30 minute waits. You can
override this by passing the `RetryWaits` option to the runner's `EnqueueJob` method.  This option
takes in a slice of durations specifying how long each wait should be. In this example we set two
retries, one after an hour and another after 6 hours:

```go
jobID, err := runner.EnqueueJob(
  "sendWelcomeEmail",
  someWelcomeData,
  pgq.RetryWaits([]time.Duration{
    time.Minute * 60,
    time.Minute * 60 * 6,
  }),
)
```

TODO
- exponential backoff runner option.
- logging, with configurable levels, as a runner option
