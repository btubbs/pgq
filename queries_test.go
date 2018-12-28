package pgq

import (
	"errors"
	"testing"
	"time"

	"github.com/guregu/null"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
)

func TestEnqueueJob(t *testing.T) {
	tt := []struct {
		desc           string
		options        []JobOption
		makeAssertions func(*testing.T, *sqlx.Tx, int, error)
	}{
		{
			desc: "simple success",
			makeAssertions: func(t *testing.T, tx *sqlx.Tx, id int, err error) {
				assert.Nil(t, err)
				// grab job from DB and compare it.
				job := &Job{}
				err = tx.Get(job, "SELECT * from pgq_jobs WHERE id = $1", id)
				assert.Equal(t, "someQueue", job.QueueName)
				assert.Equal(t, []byte("some data"), job.Data)
				assert.Equal(t, Durations{
					time.Minute,
					time.Minute,
					time.Minute,
				}, job.RetryWaits)
			},
		},
		{
			desc:    "job options called",
			options: []JobOption{func(job *Job) { panic("boom") }},
			makeAssertions: func(t *testing.T, tx *sqlx.Tx, id int, err error) {
				assert.Equal(t, "error/panic while applying option, cause: boom", err.Error())
			},
		},
	}

	db := getTestDB()
	defer db.Close()
	for _, tc := range tt {
		t.Run(tc.desc, func(t *testing.T) {
			tx, _ := db.Beginx()
			defer tx.Rollback()
			id, err := enqueueJob(tx, "someQueue", []byte("some data"), tc.options...)
			tc.makeAssertions(t, tx, id, err)
		})
	}
}

func TestGetNextJob(t *testing.T) {
	tt := []struct {
		desc          string
		enqueuedJobs  []Job
		getQueueNames []string
		expectedData  string
		jobExpected   bool
		errExpected   bool
	}{
		{
			desc: "single, ready job in queue",
			enqueuedJobs: []Job{
				{
					QueueName:  "foo",
					Data:       []byte("bar"),
					RunAfter:   time.Now(),
					RetryWaits: []time.Duration{},
				},
			},
			getQueueNames: []string{"foo"},
			jobExpected:   true,
			expectedData:  "bar",
		},
		{
			desc: "other queues excluded",
			enqueuedJobs: []Job{
				{
					QueueName:  "foo",
					Data:       []byte("bar"),
					RunAfter:   time.Now(),
					RetryWaits: []time.Duration{},
				},
			},
			getQueueNames: []string{"other foo"},
			expectedData:  "bar",
		},
		{
			desc: "unready job in queue",
			enqueuedJobs: []Job{
				{
					QueueName:  "foo",
					Data:       []byte("bar"),
					RunAfter:   time.Now().Add(time.Minute * 1),
					RetryWaits: []time.Duration{},
				},
			},
			getQueueNames: []string{"foo"},
		},
		{
			desc:          "no jobs",
			enqueuedJobs:  []Job{},
			getQueueNames: []string{"foo"},
		},
		{
			desc: "old completed jobs in queue",
			enqueuedJobs: []Job{
				{
					QueueName:  "foo",
					Data:       []byte("bar"),
					RunAfter:   time.Now(),
					RetryWaits: []time.Duration{},
					RanAt:      null.TimeFrom(time.Now().Add(time.Minute * -1)),
				},
			},
			getQueueNames: []string{"foo"},
		},
	}

	db := getTestDB()
	defer db.Close()
	for _, tc := range tt {
		t.Run(tc.desc, func(t *testing.T) {
			tx, _ := db.Beginx()
			defer tx.Rollback()
			// set up fixtures
			for _, j := range tc.enqueuedJobs {
				tx.NamedExec(`
					INSERT into pgq_jobs (
						queue_name, data, run_after, retry_waits, ran_at, error
					) VALUES (
						:queue_name, :data, :run_after, :retry_waits, :ran_at, :error
					);
				`, j)
			}
			job, err := getNextJob(tx, tc.getQueueNames)
			if tc.errExpected {
				assert.NotNil(t, err)
			}

			if tc.jobExpected {
				assert.Nil(t, err)
				assert.NotNil(t, job)
				assert.Equal(t, tc.expectedData, string(job.Data))
			}
		})
	}
}

func TestUpdateJob(t *testing.T) {
	db := getTestDB()
	defer db.Close()
	tx, _ := db.Beginx()
	defer tx.Rollback()

	var insertedJobID int64
	insertedJob := &Job{
		QueueName: "foo",
		Data:      []byte("update bar"),
		RunAfter:  time.Now(),
	}
	err := tx.QueryRow(`
		INSERT into pgq_jobs (
			queue_name, data, run_after, retry_waits
		) VALUES (
			$1, $2, $3, $4
		) RETURNING id;
	`, insertedJob.QueueName, insertedJob.Data, insertedJob.RunAfter, insertedJob.RetryWaits).Scan(&insertedJobID)
	assert.Nil(t, err)
	insertedJob.ID = insertedJobID
	ranAt := time.Now()
	err = updateJob(tx, insertedJob, ranAt, errors.New("boom"))
	assert.Nil(t, err)

	fetchedJob := &Job{}
	err = tx.Get(fetchedJob, `SELECT * from pgq_jobs;`)
	assert.Nil(t, err)
	assert.Equal(t, "update bar", string(fetchedJob.Data))
	// we lose a tiny amount of precision round tripping a timestamp through the DB
	assert.WithinDuration(t, null.TimeFrom(ranAt).Time, fetchedJob.RanAt.Time, time.Millisecond)
	assert.Equal(t, "boom", fetchedJob.Error.String)
}

func TestDeleteJob(t *testing.T) {
	db := getTestDB()
	defer db.Close()
	tx, _ := db.Beginx()
	defer tx.Rollback()

	var insertedJobID int64
	insertedJob := &Job{
		QueueName: "foo",
		Data:      []byte("update bar"),
		RunAfter:  time.Now(),
	}
	err := tx.QueryRow(`
		INSERT into pgq_jobs (
			queue_name, data, run_after, retry_waits
		) VALUES (
			$1, $2, $3, $4
		) RETURNING id;
	`, insertedJob.QueueName, insertedJob.Data, insertedJob.RunAfter, insertedJob.RetryWaits).Scan(&insertedJobID)
	assert.Nil(t, err)
	insertedJob.ID = insertedJobID

	err = deleteJob(tx, insertedJob)
	assert.Nil(t, err)

	// should be no jobs in DB
	var count int
	err = tx.QueryRow("SELECT count(*) from pgq_jobs;").Scan(&count)
	assert.Nil(t, err)
	assert.Equal(t, 0, count)
}
