package pgq

import (
	"errors"
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
				assert.Equal(t, "cannot run job, cause: no job handler registered for 'blah' queue", jobErr.Error())
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
				assert.NotNil(t, jobErr)
				assert.Equal(t, "job errored, cause: panic in job handler, cause: boom", jobErr.Error())
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
				assert.NotNil(t, jobErr)
				assert.Equal(t, "job errored, cause: boom", jobErr.Error())
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
				assert.NotNil(t, jobErr)

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
