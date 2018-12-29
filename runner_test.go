package pgq

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
)

func TestNewJobRunner(t *testing.T) {
	db := getTestDB()
	defer db.Close()
	runner := NewJobRunner(
		db.DB,
		JobPollingInterval(time.Nanosecond),
		PreserveCompletedJobs,
	)
	assert.Equal(t, false, runner.deleteJobOnComplete)
	assert.Equal(t, time.Nanosecond, runner.jobPollingInterval)
}

func TestPerformNextJob(t *testing.T) {
	tt := []struct {
		desc           string
		runnerOptions  []RunnerOption
		enqueueJobs    func(*JobRunner)
		handler        func([]byte) error
		makeAssertions func(*testing.T, *sqlx.DB, bool, error)
	}{
		{
			desc: "happy path",
			enqueueJobs: func(jr *JobRunner) {
				jr.EnqueueJob("blah", []byte("some data"))
			},
			handler: func(b []byte) error {
				return nil
			},
			makeAssertions: func(t *testing.T, db *sqlx.DB, attempted bool, jobErr error) {
				assert.True(t, attempted)
				assert.Nil(t, jobErr)
				var count int
				assert.Nil(t, db.Get(&count, `SELECT count(*) from pgq_jobs;`))
				assert.Equal(t, 0, count)
			},
		},
		{
			desc: "no job in queue",
			enqueueJobs: func(jr *JobRunner) {
			},
			handler: func(b []byte) error {
				assert.Fail(t, "I should never be called.")
				return nil
			},
			makeAssertions: func(t *testing.T, db *sqlx.DB, attempted bool, jobErr error) {
				assert.False(t, attempted)
				assert.Nil(t, jobErr)
				var count int
				assert.Nil(t, db.Get(&count, `SELECT count(*) from pgq_jobs;`))
				assert.Equal(t, 0, count)
			},
		},
		{
			desc: "handler not registered",
			enqueueJobs: func(jr *JobRunner) {
				jr.EnqueueJob("blah", []byte("some data"))
				// maybe contrived, but the only way I can think to force the runner to query for this queue
				// without registering a handler for it.
				jr.queueNames = []string{"blah"}
			},
			handler: nil,
			makeAssertions: func(t *testing.T, db *sqlx.DB, attempted bool, jobErr error) {
				assert.False(t, attempted)
				assert.Equal(t, "error performing job, cause: cannot run job, cause: no job handler registered for 'blah' queue", jobErr.Error())
				var count int
				assert.Nil(t, db.Get(&count, `SELECT count(*) from pgq_jobs;`))
				assert.Equal(t, 1, count)
			},
		},
		{
			desc: "jobFunc panics",
			enqueueJobs: func(jr *JobRunner) {
				jr.EnqueueJob("blah", []byte("some data"))
			},
			handler: func(b []byte) error {
				panic("boom")
			},
			makeAssertions: func(t *testing.T, db *sqlx.DB, attempted bool, jobErr error) {
				assert.True(t, attempted)
				assert.Nil(t, jobErr)
				var count int
				assert.Nil(t, db.Get(&count, `SELECT count(*) from pgq_jobs;`))
				assert.Equal(t, 1, count) // the retry
			},
		},
		{
			desc: "jobFunc errors",
			enqueueJobs: func(jr *JobRunner) {
				jr.EnqueueJob("blah", []byte("some data"))
			},
			handler: func(b []byte) error {
				return errors.New("boom")
			},
			makeAssertions: func(t *testing.T, db *sqlx.DB, attempted bool, jobErr error) {
				assert.True(t, attempted)
				assert.Nil(t, jobErr)
				var count int
				assert.Nil(t, db.Get(&count, `SELECT count(*) from pgq_jobs;`))
				assert.Equal(t, 1, count)
			},
		},
		{
			desc: "preserve attempts",
			enqueueJobs: func(jr *JobRunner) {
				jr.EnqueueJob("blah", []byte("some data"))
			},
			runnerOptions: []RunnerOption{
				PreserveCompletedJobs,
			},
			handler: func(b []byte) error {
				return nil
			},
			makeAssertions: func(t *testing.T, db *sqlx.DB, attempted bool, jobErr error) {
				assert.True(t, attempted)
				assert.Nil(t, jobErr)

				jobs := []Job{}
				assert.Nil(t, db.Select(&jobs, `SELECT * FROM pgq_jobs ORDER BY created_at`))
				assert.Equal(t, 1, len(jobs))
				// a successful job that's left in the table will have a timestamp in ran_at, but a
				// null in error.
				assert.True(t, jobs[0].RanAt.Valid)
				assert.False(t, jobs[0].Error.Valid)
			},
		},
		{
			desc: "preserve errors with attempts",
			enqueueJobs: func(jr *JobRunner) {
				jr.EnqueueJob("blah", []byte("some data"))
			},
			runnerOptions: []RunnerOption{
				PreserveCompletedJobs,
			},
			handler: func(b []byte) error {
				return errors.New("boom")
			},
			makeAssertions: func(t *testing.T, db *sqlx.DB, attempted bool, jobErr error) {
				assert.True(t, attempted)
				assert.Nil(t, jobErr)

				jobs := []Job{}
				assert.Nil(t, db.Select(&jobs, `SELECT * FROM pgq_jobs ORDER BY created_at`))
				assert.Equal(t, 2, len(jobs))

				savedJob := jobs[0]
				assert.True(t, savedJob.Error.Valid)
				assert.Equal(t, "boom", savedJob.Error.String)
				assert.True(t, savedJob.RanAt.Valid)
			},
		},
		{
			desc: "retries get enqueued",
			enqueueJobs: func(jr *JobRunner) {
				jr.EnqueueJob("blah", []byte("some data"))
			},
			handler: func(b []byte) error {
				return errors.New("boom")
			},
			makeAssertions: func(t *testing.T, db *sqlx.DB, attempted bool, jobErr error) {
				assert.True(t, attempted)

				jobs := []Job{}
				assert.Nil(t, db.Select(&jobs, `SELECT * FROM pgq_jobs`))
				assert.Equal(t, 1, len(jobs))
				assert.Equal(t, Durations{
					time.Minute * 10,
					time.Minute * 30,
				}, jobs[0].RetryWaits)
			},
		},
	}
	for _, tc := range tt {
		t.Run(tc.desc, func(t *testing.T) {
			withFreshDB(func(db *sqlx.DB) {
				runner := NewJobRunner(
					db.DB,
					tc.runnerOptions...,
				)
				tc.enqueueJobs(runner)
				if tc.handler != nil {
					err := runner.RegisterQueue("blah", tc.handler)
					assert.Nil(t, err)
				}
				attempted, err := runner.PerformNextJob()
				tc.makeAssertions(t, db, attempted, err)
			})
		})
	}
}

func TestRunABunchOfTasks(t *testing.T) {
	withFreshDB(func(db *sqlx.DB) {

		jobMultiplier := 100
		retries := []time.Duration{0}
		expectedJobs := (jobMultiplier + // "good" attempts
			jobMultiplier*(len(retries)+1) + // "bad" attempts
			jobMultiplier*(len(retries)+1)) // "ugly" attempts
		var jobWG sync.WaitGroup
		jobWG.Add(expectedJobs)

		// we also say "done" when a runner exits
		runnerCount := 1
		var runnerWG sync.WaitGroup
		runnerWG.Add(runnerCount)

		// three tasks.  one always succeeds, one always fails, one always panics
		good := func(data []byte) error {
			jobWG.Done()
			return nil
		}
		bad := func(data []byte) error {
			jobWG.Done()
			return errors.New("this is an error")
		}
		ugly := func(data []byte) error {
			jobWG.Done()
			panic("this is a panic!")
		}

		var runners []*JobRunner
		// start up 10 runners.  persist job history
		for n := 0; n < runnerCount; n++ {
			jr := NewJobRunner(
				db.DB,
				OnStop(func() {
					runnerWG.Done()
				}),
				PreserveCompletedJobs,
				JobPollingInterval(0),
			)
			jr.RegisterQueue("good", good)
			jr.RegisterQueue("bad", bad)
			jr.RegisterQueue("ugly", ugly)
			go jr.Run()
			runners = append(runners, jr)
		}
		for n := 0; n < jobMultiplier; n++ {
			_, err := runners[0].EnqueueJob(
				"good",
				[]byte(""),
				RetryWaits(retries),
			)
			assert.Nil(t, err)
			runners[0].EnqueueJob(
				"bad",
				[]byte(""),
				RetryWaits(retries),
			)
			assert.Nil(t, err)
			runners[0].EnqueueJob(
				"ugly",
				[]byte(""),
				RetryWaits([]time.Duration{0}),
			)
			assert.Nil(t, err)
		}

		jobWG.Wait()

		// tell all the runners to stop
		for _, runner := range runners {
			runner.StopChan <- true
		}
		runnerWG.Wait()

		var jobCount int
		// total attempts
		err := db.QueryRow(`SELECT count(*) FROM pgq_jobs`).Scan(&jobCount)
		assert.Nil(t, err)
		assert.Equal(t, expectedJobs, jobCount)

		// good
		err = db.QueryRow(`SELECT count(*) FROM pgq_jobs WHERE queue_name='good'`).Scan(&jobCount)
		assert.Nil(t, err)
		assert.Equal(t, jobMultiplier, jobCount)

		// bad
		err = db.QueryRow(`SELECT count(*) FROM pgq_jobs WHERE queue_name='bad'`).Scan(&jobCount)
		assert.Nil(t, err)
		assert.Equal(t, jobMultiplier*(len(retries)+1), jobCount)

		// ugly
		err = db.QueryRow(`SELECT count(*) FROM pgq_jobs WHERE queue_name='ugly'`).Scan(&jobCount)
		assert.Nil(t, err)
		assert.Equal(t, jobMultiplier*(len(retries)+1), jobCount)
	})
}
