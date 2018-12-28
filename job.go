package pgq

import (
	"fmt"
	"time"

	"github.com/guregu/null"
)

// Job contains all the info needed to execute a single job attempt.
type Job struct {
	ID         int64       `db:"id"`
	CreatedAt  time.Time   `db:"created_at"`
	QueueName  string      `db:"queue_name"`
	Data       []byte      `db:"data"`
	RunAfter   time.Time   `db:"run_after"`
	RetryWaits Durations   `db:"retry_waits"`
	RanAt      null.Time   `db:"ran_at"`
	Error      null.String `db:"error"`
}

// A JobOption sets an optional parameter on a Job that you're enqueueing.
type JobOption func(*Job)

// After sets the timestamp after which the Job may run.
func After(t time.Time) JobOption {
	return func(j *Job) {
		j.RunAfter = t
	}
}

// RetryWaits explicitly sets the wait periods for the Job's retries.
func RetryWaits(ds []time.Duration) JobOption {
	return func(j *Job) {
		j.RetryWaits = Durations(ds)
	}
}

func applyJobOption(jo JobOption, job *Job) (outErr error) {
	defer func() {
		if r := recover(); r != nil {
			var ok bool
			outErr, ok = r.(error)
			if !ok {
				outErr = fmt.Errorf("%v", r)
			}
		}
	}()
	jo(job)
	return nil
}
