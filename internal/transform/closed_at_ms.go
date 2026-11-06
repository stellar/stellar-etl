package transform

import "time"

// closedAtMsToTime converts the millisecond close time the SDK returns into the
// UTC timestamp that the closed_at_ms column carries.
//
// BigQuery stores closed_at_ms as a TIMESTAMP, which is microsecond precision,
// so every millisecond value round trips exactly. A pre-Protocol-30 ledger
// closed on a whole second and therefore renders identically to closed_at; only
// a CAP-88 arm produces a value with a non-zero millisecond part.
func closedAtMsToTime(ms int64) time.Time {
	return time.UnixMilli(ms).UTC()
}
