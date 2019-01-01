package pgq

// Backoffer is a type of error that can also indicate whether a queue should slow down.
type Backoffer interface {
	error
	Backoff() bool
}

// BackoffError is an implementation of the Backoff interface
type BackoffError struct {
	message       string
	shouldBackoff bool
}

// Error implements the error interface.
func (b BackoffError) Error() string {
	return b.message
}

// Backoff implements the Backoffer interface.
func (b BackoffError) Backoff() bool {
	return b.shouldBackoff
}

// Backoff returns an error that implements the Backoffer interface, telling the caller
// processing the error to slow the queue down.
func Backoff(msg string) Backoffer {
	return BackoffError{
		message:       msg,
		shouldBackoff: true,
	}
}
