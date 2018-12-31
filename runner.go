package pgq

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/joomcode/errorx"

	// We only work with Postgres, so might as well pre-register the driver.
	_ "github.com/lib/pq"
)

var (
	minBackoff = time.Millisecond * 100
	maxBackoff = time.Second * 64
)

// JobRunner provides methods for putting jobs on a Postgres-backed queue, and performing any jobs
// that are there.
type JobRunner struct {
	db                  *sqlx.DB
	queues              map[string]*queue
	jobPollingInterval  time.Duration
	deleteJobOnComplete bool
	StopChan            chan bool
	onStop              func()
}

type queue struct {
	handler     func([]byte) error
	pausedUntil time.Time
	backoff     time.Duration
}

// NewJobRunner takes a Postgres DB connection and returns a JobRunner instance.
func NewJobRunner(db *sql.DB, options ...RunnerOption) *JobRunner {
	runner := &JobRunner{
		db:                  sqlx.NewDb(db, "postgres"),
		queues:              map[string]*queue{},
		jobPollingInterval:  time.Second * 10,
		deleteJobOnComplete: true,
		StopChan:            make(chan bool),
	}
	for _, option := range options {
		option(runner)
	}
	return runner
}

// EnqueueJob puts a job on the queue.  If successful, it returns the Job ID.
func (jr *JobRunner) EnqueueJob(queueName string, data []byte, options ...JobOption) (int, error) {
	return enqueueJob(jr.db, queueName, data, options...)
}

// EnqueueJobInTx enqueues a Job, but lets you provide your own sql.Tx or other compatible object
// with an Exec method.  This is useful if your application has other tables in the same database,
// and you want to only enqueue the job if all the DB operations in the same transaction are
// successful.  All the handling of Begin, Commit, and Rollback calls is up to you.
func (jr *JobRunner) EnqueueJobInTx(tx DB, queueName string, data []byte, options ...JobOption) (int, error) {
	return enqueueJob(tx, queueName, data, options...)
}

// RegisterQueue tells your JobRunner instance which function should be called for a
// given job type.
func (jr *JobRunner) RegisterQueue(queueName string, jobFunc func([]byte) error) error {
	if _, alreadyRegistered := jr.queues[queueName]; alreadyRegistered {
		return fmt.Errorf("a handler for %s jobs has already been registered", queueName)
	}
	jr.queues[queueName] = &queue{handler: jobFunc}
	return nil
}

// Run will query for the next job in the queue, then run it, then do another, forever.
func (jr *JobRunner) Run() error {
	if jr.onStop != nil {
		defer jr.onStop()
	}
	for {
		select {
		case <-jr.StopChan:
			return nil
		default:
			if foundJob, err := jr.PerformNextJob(); err != nil {
				return errorx.Decorate(err, "exiting job runner")
			} else if !foundJob {
				// we didn't find a job.  Take a nap.
				time.Sleep(jr.jobPollingInterval)
			}
		}
	}
}

func (jr *JobRunner) getQueueNames() []string {
	names := []string{}
	now := time.Now()
	for k, v := range jr.queues {
		if v.pausedUntil.Before(now) {
			names = append(names, k)
		}
	}
	return names
}

// PerformNextJob performs the next job in the queue. It returns true if it attempted to run a job,
// or false if there was no job in the queue or some error prevented it from attempting to run the
// job.  It returns two errors: jobErr, which is the error raised from the jobFunc itself (if any),
// and any error raised in the surrounding code.
func (jr *JobRunner) PerformNextJob() (found bool, outErr error) {
	var jobErr error // the error returned by the jobFunc
	tx, err := jr.db.Beginx()
	if err != nil {
		return false, err
	}
	defer func() {
		outErr = errorx.DecorateMany("error performing job", outErr, tx.Commit())
	}()

	// get job
	queueNames := jr.getQueueNames()
	if len(queueNames) == 0 {
		return false, nil
	}

	job, err := getNextJob(tx, queueNames)
	if err != nil {
		return false, err
	}

	// nothing to do.  Bail out here.
	if job == nil {
		return false, nil
	}

	// get handler func from internal map
	queue, ok := jr.queues[job.QueueName]
	if !ok {
		return false, errorx.DecorateMany(
			"cannot run job",
			fmt.Errorf("no job handler registered for '%s' queue", job.QueueName),
		)
	}
	ranAt := time.Now()

	// run the job func in its own closure with its own panic handler.
	func() {
		defer func() {
			if r := recover(); r != nil {
				panicErr := fmt.Errorf("%v", r)
				jobErr = errorx.DecorateMany("panic in job handler", jobErr, panicErr)
			}
		}()
		jobErr = queue.handler(job.Data)
	}()

	// either delete the job from the queue, or update it with output, depending on how we've been
	// configured.
	if jr.deleteJobOnComplete {
		err = deleteJob(tx, job)
		if err != nil {
			return true, errorx.Decorate(err, "could not delete job")
		}
	} else {
		// store the ranAt time and any error returned
		err = updateJob(tx, job, ranAt, jobErr)
		if err != nil {
			return true, errorx.Decorate(err, "could not update job")
		}
	}

	if jobErr != nil {
		// handle backoffs
		if b, ok := jobErr.(Backoffer); ok && b.Backoff() {
			// change multiplier if necessary
			if queue.backoff == 0 {
				queue.backoff = minBackoff
			} else {
				queue.backoff *= 2
			}

			if queue.backoff > maxBackoff {
				queue.backoff = maxBackoff
			}
		}
		// handle retries
		if len(job.RetryWaits) > 0 {
			// we errored, but we have more attempts.  Enqueue the next one for the future, after waiting
			// the first attempt duration.  Store the rest of the attempt Durations on the new Job.
			_, err = enqueueJob(
				tx,
				job.QueueName,
				job.Data,
				After(time.Now().Add(job.RetryWaits[0])),
				RetryWaits(job.RetryWaits[1:]),
			)
			if err != nil {
				return true, errorx.Decorate(err, "error enqueueing retry")
			}
		}
	}
	queue.pausedUntil = ranAt.Add(queue.backoff)
	return true, nil
}

// A RunnerOption sets an optional parameter on the JobRunner.
type RunnerOption func(*JobRunner)

// JobPollingInterval sets the amount of time that the runner will sleep if it has no jobs to do.
// Default is 10 seconds.
func JobPollingInterval(d time.Duration) RunnerOption {
	return func(jr *JobRunner) {
		jr.jobPollingInterval = d
	}
}

// PreserveCompletedJobs sets the runner option to leave job attempts in the pgq_jobs table instead
// of deleting them when complete.
func PreserveCompletedJobs(jr *JobRunner) {
	jr.deleteJobOnComplete = false
}

// OnStop sets an optional callback function that will be called when the runner exits its Run
// method.
func OnStop(f func()) RunnerOption {
	return func(jr *JobRunner) {
		jr.onStop = f
	}
}
