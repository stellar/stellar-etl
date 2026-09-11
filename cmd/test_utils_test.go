package cmd

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestExtractErrorMsg covers the helper every WantErr assertion in this package
// runs through. The fixtures are real lines captured from the CLI, so a change
// to the log envelope that would silently break those assertions fails here
// first, with a readable diff instead of 63 mismatched error strings.
func TestExtractErrorMsg(t *testing.T) {
	fatalLine := `{"component":"get_ledger_range_from_times","level":"fatal",` +
		`"message":"could not parse start time: parsing time \"bad\" as \"2006-01-02T15:04:05-07:00\": cannot parse \"bad\" as \"2006\"",` +
		`"msg":"could not parse start time: parsing time \"bad\" as \"2006-01-02T15:04:05-07:00\": cannot parse \"bad\" as \"2006\"",` +
		`"pid":1,"service":"stellar-etl","severity":"CRITICAL","time":"2026-09-11T10:13:45.075-07:00"}`
	wantFatal := `could not parse start time: parsing time "bad" as "2006-01-02T15:04:05-07:00": cannot parse "bad" as "2006"`

	errorLine := `{"component":"export_ledgers","context":{"strict_export":false},"level":"error",` +
		`"message":"could not transform ledger 58231044","msg":"could not transform ledger 58231044",` +
		`"pid":1,"service":"stellar-etl","severity":"ERROR","time":"2026-09-11T10:13:45.075-07:00"}`

	tests := []struct {
		name  string
		input string
		want  string
	}{
		{
			name:  "single fatal line",
			input: fatalLine + "\n",
			want:  wantFatal,
		},
		{
			name:  "quotes inside the message survive unescaping",
			input: fatalLine,
			want:  wantFatal,
		},
		{
			// A non-strict export logs every failed record before it ends, and
			// the assertions are written against the first one.
			name:  "first message wins when several are logged",
			input: errorLine + "\n" + fatalLine + "\n",
			want:  "could not transform ledger 58231044",
		},
		{
			// Anything the CLI prints outside the logger, such as cobra's own
			// usage output, must not be mistaken for the error.
			name:  "non-JSON noise is skipped",
			input: "Using config file: /home/.stellar-etl.yaml\n" + errorLine + "\n",
			want:  "could not transform ledger 58231044",
		},
		{
			name:  "empty output yields an empty message",
			input: "",
			want:  "",
		},
		{
			name:  "JSON without a message yields an empty message",
			input: `{"severity":"INFO"}` + "\n",
			want:  "",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.want, extractErrorMsg(test.input))
		})
	}
}
