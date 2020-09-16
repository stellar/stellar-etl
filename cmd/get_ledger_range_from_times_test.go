package cmd

import (
	"fmt"
	"testing"
)

func TestConvertTimes(t *testing.T) {
	tests := []cliTest{
		{
			name:    "wrong date format",
			args:    []string{"get_ledger_range_from_times", "-s", "2016 01 01 4:33", "-e", "2020 03 04 12:32", "--stdout"},
			golden:  "",
			wantErr: fmt.Errorf("could not parse start time: parsing time \\"),
		},
		{
			name:    "normal range",
			args:    []string{"get_ledger_range_from_times", "-s", "2016-11-10T18:00:00-05:00", "-e", "2019-09-13T23:00:00+00:00", "--stdout"},
			golden:  "normal_range.golden",
			wantErr: nil,
		},
		{
			name:    "start too early",
			args:    []string{"get_ledger_range_from_times", "-s", "2006-11-10T18:00:00-05:00", "-e", "2019-09-13T23:00:00+00:00", "--stdout"},
			golden:  "early_start.golden",
			wantErr: nil,
		},
		{
			name:    "start too late",
			args:    []string{"get_ledger_range_from_times", "-s", "2021-09-13T23:00:00+00:00", "-e", "2021-09-13T23:30:00+00:00", "--stdout"},
			golden:  "late_start.golden",
			wantErr: nil,
		},
		{
			name:    "end too late",
			args:    []string{"get_ledger_range_from_times", "-s", "2017-11-10T12:14:32+04:00", "-e", "2021-09-13T23:00:00+00:00", "--stdout"},
			golden:  "late_end.golden",
			wantErr: nil,
		},
		{
			name:    "end too early",
			args:    []string{"get_ledger_range_from_times", "-s", "2006-11-10T12:14:32+04:00", "-e", "2006-11-10T12:14:32+04:00", "--stdout"},
			golden:  "early_end.golden",
			wantErr: nil,
		},
		{
			name:    "same date",
			args:    []string{"get_ledger_range_from_times", "-s", "2016-11-10T18:03:37-05:00", "-e", "2016-11-10T18:03:37-05:00", "--stdout"},
			golden:  "same_date.golden",
			wantErr: nil,
		},
		{
			name:    "checkpoint range (22343680-22343743)",
			args:    []string{"get_ledger_range_from_times", "-s", "2019-02-06T09:14:43+00:00", "-e", "2019-02-06T09:20:23+00:00", "--stdout"},
			golden:  "checkpoint_range.golden",
			wantErr: nil,
		},
		{
			name:    "checkpoint range (9341-9401)",
			args:    []string{"get_ledger_range_from_times", "-s", "2015-10-01T06:20:00+00:00", "-e", "2015-10-01T06:25:00+00:00", "--stdout"},
			golden:  "checkpoint_range2.golden",
			wantErr: nil,
		},
		{
			name:    "early checkpoint range (3-3)",
			args:    []string{"get_ledger_range_from_times", "-s", "2015-09-30T16:50:00+00:00", "-e", "2015-09-30T16:55:00+00:00", "--stdout"},
			golden:  "early_checkpoint_range.golden",
			wantErr: nil,
		},
	}

	for _, test := range tests {
		runCLITest(t, test, "testdata/ranges/")
	}
}
