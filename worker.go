// Package pgq provides an implementation of a Postgres-backed job queue.  Safe concurrency is built
// on top of the SKIP LOCKED functionality introduced in Postgres 9.5.  Retries and exponential
// backoff are supported.
package pgq

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/joomcode/errorx"
	log "github.com/sirupsen/logrus"

	// We only work with Postgres, so might as well pre-register the driver.
	_ "github.com/lib/pq"
)

var (
	minBackoff = time.Millisecond * 100
	maxBackoff = time.Minute
)

// Worker provides methods for putting jobs on a Postgres-backed queue, and performing any jobs
// that are there.
type Worker struct {
	db                  *sqlx.DB
	queues              map[string]*queue
	jobPollingInterval  time.Duration
	deleteJobOnComplete bool
	StopChan            chan bool
	onStop              func()
	log                 *log.Logger
}

type queue struct {
	handler     func([]byte) error
	pausedUntil time.Time
	backoff     time.Duration
}

// NewWorker takes a Postgres DB connection and returns a Worker instance.
func NewWorker(db *sql.DB, options ...WorkerOption) *Worker {
	runner := &Worker{
		StopChan:            make(chan bool),
		db:                  sqlx.NewDb(db, "postgres"),
		queues:              map[string]*queue{},
		jobPollingInterval:  time.Second * 10,
		deleteJobOnComplete: true,
		log:                 defaultLogger(),
	}
	for _, option := range options {
		option(runner)
	}
	return runner
}

// EnqueueJob puts a job on the queue.  If successful, it returns the Job ID.
func (worker *Worker) EnqueueJob(queueName string, data []byte, options ...JobOption) (int, error) {
	id, err := enqueueJob(worker.db, queueName, data, options...)
	logEntry := worker.log.WithFields(log.Fields{
		"id":        id,
		"queueName": queueName,
	})
	if err != nil {
		logEntry.WithField("error", err).Error("EnqueueJob")
	} else {
		logEntry.Info("EnqueueJob")
	}
	return id, err
}

// EnqueueJobInTx enqueues a Job, but lets you provide your own sql.Tx or other compatible object
// with an Exec method.  This is useful if your application has other tables in the same database,
// and you want to only enqueue the job if all the DB operations in the same transaction are
// successful.  All the handling of Begin, Commit, and Rollback calls is up to you.
func (worker *Worker) EnqueueJobInTx(tx DB, queueName string, data []byte, options ...JobOption) (int, error) {
	id, err := enqueueJob(tx, queueName, data, options...)
	logEntry := worker.log.WithFields(log.Fields{
		"id":        id,
		"queueName": queueName,
	})
	if err != nil {
		logEntry.WithField("error", err).Error("EnqueueJobInTx")
	} else {
		logEntry.Info("EnqueueJobInTx")
	}
	return id, err
}

// RegisterQueue tells your Worker instance which function should be called for a
// given job type.
func (worker *Worker) RegisterQueue(queueName string, jobFunc func([]byte) error) error {
	if _, alreadyRegistered := worker.queues[queueName]; alreadyRegistered {
		return fmt.Errorf("a handler for %s jobs has already been registered", queueName)
	}
	worker.queues[queueName] = &queue{handler: jobFunc}
	return nil
}

// Run will query for the next job in the queue, then run it, then do another, forever.
func (worker *Worker) Run() error {
	worker.log.WithField("queueNames", worker.getQueueNames()).Info("Run")
	defer func() {
		worker.log.Info("Exiting")
		if worker.onStop != nil {
			worker.onStop()
		}
	}()
	for {
		select {
		case <-worker.StopChan:
			return nil
		default:
			if attemptedJob, err := worker.PerformNextJob(); err != nil {
				return errorx.Decorate(err, "exiting job runner")
			} else if !attemptedJob {
				// we didn't find a job.  Take a nap.
				time.Sleep(worker.jobPollingInterval)
			}
		}
	}
}

func (worker *Worker) getQueueNames() []string {
	names := []string{}
	now := time.Now()
	for k, v := range worker.queues {
		if v.pausedUntil.Before(now) {
			names = append(names, k)
		}
	}
	return names
}

// PerformNextJob performs the next job in the queue. It returns true if it attempted to run a job, or false
// if there was no job in the queue or some error prevented it from attempting to run the job.  It only returns an
// error if there's some problem talking to Postgres.  Errors inside jobs are not bubbled up.
func (worker *Worker) PerformNextJob() (attempted bool, outErr error) {
	var jobErr error // the error returned by the jobFunc

	// start an empty log entry that we'll append to throughout this func
	logEntry := worker.log.WithFields(log.Fields{})
	tx, err := worker.db.Beginx()
	if err != nil {
		return false, err
	}
	defer func() {
		logEntry = logEntry.WithFields(log.Fields{
			"jobFound": attempted,
		})

		if jobErr != nil {
			logEntry = logEntry.WithField("jobError", jobErr)
		}

		if outErr != nil {
			logEntry = logEntry.WithField("workerError", outErr)
		}

		if jobErr != nil || outErr != nil {
			logEntry.Error("PerformNextJob")
		} else {
			logEntry.Info("PerformNextJob")
		}

		outErr = errorx.DecorateMany("error performing job", outErr, tx.Commit())
	}()

	// get job
	queueNames := worker.getQueueNames()
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
	logEntry = logEntry.WithFields(log.Fields{"id": job.ID, "queueName": job.QueueName})

	// get handler func from internal map
	queue, ok := worker.queues[job.QueueName]
	if !ok {
		return false, errorx.DecorateMany(
			"cannot run job",
			fmt.Errorf("no job handler registered for '%s' queue", job.QueueName),
		)
	}
	ranAt := time.Now()
	logEntry = logEntry.WithTime(ranAt)

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
	if worker.deleteJobOnComplete {
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
			logEntry = logEntry.WithField("backoff", true)
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
			afterTime := time.Now().Add(job.RetryWaits[0])
			logEntry = logEntry.WithField("retryAfter", afterTime)
			_, err = enqueueJob(
				tx,
				job.QueueName,
				job.Data,
				After(afterTime),
				RetryWaits(job.RetryWaits[1:]),
			)
			if err != nil {
				return true, errorx.Decorate(err, "error enqueueing retry")
			}
		}
	}
	if queue.backoff > 0 {
		queue.pausedUntil = ranAt.Add(queue.backoff)
		logEntry = logEntry.WithField("queuePausedUntil", queue.pausedUntil)
	}
	return true, nil
}

// A WorkerOption sets an optional parameter on the Worker.
type WorkerOption func(*Worker)

// JobPollingInterval sets the amount of time that the runner will sleep if it has no jobs to do.
// Default is 10 seconds.
func JobPollingInterval(d time.Duration) WorkerOption {
	return func(worker *Worker) {
		worker.jobPollingInterval = d
	}
}

// PreserveCompletedJobs sets the runner option to leave job attempts in the pgq_jobs table instead
// of deleting them when complete.
func PreserveCompletedJobs(worker *Worker) {
	worker.deleteJobOnComplete = false
}

// OnStop sets an optional callback function that will be called when the runner exits its Run
// method.
func OnStop(f func()) WorkerOption {
	return func(worker *Worker) {
		worker.onStop = f
	}
}

// SetLogger allows you to set your own logrus logger object for use by the job worker.
func SetLogger(l *log.Logger) WorkerOption {
	return func(worker *Worker) {
		worker.log = l
	}
}

func defaultLogger() *log.Logger {
	formatter := &log.TextFormatter{
		FullTimestamp: true,
	}
	logger := log.New()
	logger.Formatter = formatter
	return logger
}
