package pgq

import "time"

type backoff struct {
	endsAt time.Time
}
