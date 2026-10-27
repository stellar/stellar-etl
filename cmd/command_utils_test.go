package cmd

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/guregu/null"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type exportEntryTestRow struct {
	ID       int64       `json:"id"`
	Name     string      `json:"name"`
	Nullable null.String `json:"nullable"`
	NullInt  null.Int    `json:"null_int"`
	Details  interface{} `json:"details"`
}

func TestExportEntry(t *testing.T) {
	row := exportEntryTestRow{
		ID:       9223372036854775807,
		Name:     "row",
		Nullable: null.StringFrom("set"),
		Details:  map[string]interface{}{"name": "nested"},
	}

	tests := []struct {
		name  string
		entry interface{}
		extra map[string]string
		want  string
	}{
		{
			name:  "no extra fields",
			entry: row,
			want:  `{"id":9223372036854775807,"name":"row","nullable":"set","null_int":null,"details":{"name":"nested"}}`,
		},
		{
			name:  "extra fields appended",
			entry: row,
			extra: map[string]string{"batch_id": "b1", "batch_run_date": "2026-10-21"},
			want:  `{"id":9223372036854775807,"name":"row","nullable":"set","null_int":null,"details":{"name":"nested"},"batch_id":"b1","batch_run_date":"2026-10-21"}`,
		},
		{
			name:  "extra field overrides an entry field",
			entry: row,
			extra: map[string]string{"name": "override"},
			want:  `{"details":{"name":"nested"},"id":9223372036854775807,"name":"override","null_int":null,"nullable":"set"}`,
		},
		{
			name:  "extra key matching a nested key",
			entry: row,
			extra: map[string]string{"batch_id": "b1", "name": "override"},
			want:  `{"batch_id":"b1","details":{"name":"nested"},"id":9223372036854775807,"name":"override","null_int":null,"nullable":"set"}`,
		},
		{
			name:  "empty entry",
			entry: struct{}{},
			extra: map[string]string{"batch_id": "b1"},
			want:  `{"batch_id":"b1"}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			outFile, err := os.Create(filepath.Join(t.TempDir(), "out.txt"))
			require.NoError(t, err)
			defer outFile.Close()

			numBytes, err := ExportEntry(tt.entry, outFile, tt.extra)
			require.NoError(t, err)

			got, err := os.ReadFile(outFile.Name())
			require.NoError(t, err)
			assert.Equal(t, tt.want+"\n", string(got))
			assert.Equal(t, len(got), numBytes)
		})
	}
}

func TestExportEntryRejectsNonObject(t *testing.T) {
	outFile, err := os.Create(filepath.Join(t.TempDir(), "out.txt"))
	require.NoError(t, err)
	defer outFile.Close()

	_, err = ExportEntry([]int{1}, outFile, map[string]string{"batch_id": "b1"})
	assert.Error(t, err)
}

func TestSortJSONKeys(t *testing.T) {
	in := "{\"b\":1,\"a\":{\"d\":2,\"c\":12345678901234567890}}\nnot json\n"
	want := "{\"a\":{\"c\":12345678901234567890,\"d\":2},\"b\":1}\nnot json\n"
	assert.Equal(t, want, sortJSONKeys(in))
}
