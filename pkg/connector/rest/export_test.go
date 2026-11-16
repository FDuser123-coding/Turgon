package rest

import "time"

// SetClock sets the time an event's lookback counts back from, for tests
// in package rest_test. It is not part of the package's API.
func (c *Conn) SetClock(now func() time.Time) { c.clock = now }
