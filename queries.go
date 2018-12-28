package pgq

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/guregu/null"
	"github.com/jmoiron/sqlx"
	"github.com/joomcode/errorx"
)

func getNextJob(tx *sqlx.Tx, queueNames []string) (*Job, error) {
	query, args, err := sqlx.In(`
		SELECT * FROM pgq_jobs
		WHERE
			queue_name IN (?)
			AND run_after < ?
			AND ran_at IS NULL
		ORDER BY run_after
		LIMIT 1
		FOR UPDATE SKIP LOCKED;`,
		queueNames,
		time.Now(),
	)
	if err != nil {
		return nil, errorx.Decorate(err, "could not create job query")
	}
	query = tx.Rebind(query)
	job := &Job{}
	err = tx.Get(job, query, args...)

	switch err {
	case nil:
		return job, nil
	case sql.ErrNoRows:
		return nil, nil
	default:
		return nil, errorx.Decorate(err, "could not get next job")
	}
}

// DB is anything with the DB methods on it that we need. (like a DB or a Tx)
type DB interface {
	Exec(string, ...interface{}) (sql.Result, error)
	QueryRow(string, ...interface{}) *sql.Row
}

func enqueueJob(execer DB, queueName string, data []byte, options ...JobOption) (int, error) {
	// create job with provided data and default options
	job := &Job{
		QueueName: queueName,
		Data:      data,
		RunAfter:  time.Now(),
		// by default, we'll do 3 attempts with 60 seconds between each.
		RetryWaits: []time.Duration{
			time.Second * 60,
			time.Second * 60 * 10,
			time.Second * 60 * 30,
		},
	}
	// Apply any job customzations provided by the user
	for _, option := range options {
		err := applyJobOption(option, job)
		if err != nil {
			return 0, errorx.Decorate(err, "error/panic while applying option")
		}
	}
	// persist
	var jobID int
	err := execer.QueryRow(`
			INSERT INTO pgq_jobs (
				queue_name,
				data,
				run_after,
				retry_waits
			) VALUES (
				$1,
				$2,
				$3,
				$4
			) RETURNING id;
	`, job.QueueName, job.Data, job.RunAfter, job.RetryWaits).Scan(&jobID)
	return jobID, errorx.DecorateMany("could not enqueue job", err)
}

func updateJob(execer DB, job *Job, ranAt time.Time, jobErr error) error {
	job.RanAt = null.TimeFrom(ranAt)
	if jobErr != nil {
		job.Error = null.StringFrom(jobErr.Error())
	}
	result, err := execer.Exec(`
		UPDATE pgq_jobs SET
			ran_at = $1,
			error = $2
		WHERE
			id = $3;`,
		job.RanAt,
		job.Error,
		job.ID,
	)
	if err != nil || result == nil {
		return errorx.Decorate(err, "could not update job with result")
	}
	affected, affectedErr := result.RowsAffected()
	if affected != 1 {
		return fmt.Errorf("expected to update 1 job, but updated %d", affected)
	}
	return errorx.DecorateMany("could not get rows affected", affectedErr)
}

func deleteJob(execer DB, job *Job) error {
	result, err := execer.Exec(`
		DELETE from pgq_jobs
		WHERE id = $1;
	`, job.ID)
	if err != nil || result == nil {
		return errorx.Decorate(err, "could not delete job")
	}
	affected, affectedErr := result.RowsAffected()
	if affected != 1 {
		return fmt.Errorf("expected to delete 1 job, but deleted %d", affected)
	}
	return errorx.DecorateMany("could not get rows affected", affectedErr)
}
