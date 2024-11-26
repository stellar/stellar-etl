package cmd

import (
	"fmt"
	"testing"
)

func TestConvertTimes(t *testing.T) {
	tests := []CliTest{
		{
			Name:    "wrong date format",
			Args:    []string{"get_ledger_range_from_times", "-s", "2016 01 01 4:33", "-e", "2020 03 04 12:32"},
			Golden:  "",
			WantErr: fmt.Errorf("could not parse start time: parsing time \\"),
		},
		{
			Name:    "normal range",
			Args:    []string{"get_ledger_range_from_times", "-s", "2016-11-10T18:00:00-05:00", "-e", "2019-09-13T23:00:00+00:00", "-o", GotTestDir(t, "normal_range.txt")},
			Golden:  "normal_range.golden",
			WantErr: nil,
		},
		{
			Name:    "start too early",
			Args:    []string{"get_ledger_range_from_times", "-s", "2006-11-10T18:00:00-05:00", "-e", "2019-09-13T23:00:00+00:00", "-o", GotTestDir(t, "early_start.txt")},
			Golden:  "early_start.golden",
			WantErr: nil,
		},
		// {
		// 	Name: "start too late",
		// 	// @TODO
		// 	// assertion should actually be that the start and end times equal
		// 	// since it always grabs the end ledger you cannot hardcode the expected result
		// 	// maybe grab the latest ledger through code??
		// 	Args:    []string{"get_ledger_range_from_times", "-s", "2021-09-13T23:00:00+00:00", "-e", "2021-09-13T23:30:00+00:00"},
		// 	Golden:  "late_start.golden",
		// 	WantErr: nil,
		// },
		// {
		// 	Name: "end too late",
		// 	// @TODO
		// 	// Change the expected output to the max ledger time
		// 	// cannot be hardcoded in a golden ledger
		// 	Args:    []string{"get_ledger_range_from_times", "-s", "2017-11-10T12:14:32+04:00", "-e", "2021-09-13T23:00:00+00:00"},
		// 	Golden:  "late_end.golden",
		// 	WantErr: nil,
		// },
		{
			Name:    "end too early",
			Args:    []string{"get_ledger_range_from_times", "-s", "2006-11-10T12:14:32+04:00", "-e", "2006-11-10T12:14:32+04:00", "-o", GotTestDir(t, "early_end.txt")},
			Golden:  "early_end.golden",
			WantErr: nil,
		},
		{
			Name:    "same date",
			Args:    []string{"get_ledger_range_from_times", "-s", "2016-11-10T18:03:37-05:00", "-e", "2016-11-10T18:03:37-05:00", "-o", GotTestDir(t, "same_date.txt")},
			Golden:  "same_date.golden",
			WantErr: nil,
		},
		{
			Name:    "checkpoint range (22343680-22343743)",
			Args:    []string{"get_ledger_range_from_times", "-s", "2019-02-06T09:14:43+00:00", "-e", "2019-02-06T09:20:23+00:00", "-o", GotTestDir(t, "checkpoint_range.txt")},
			Golden:  "checkpoint_range.golden",
			WantErr: nil,
		},
		{
			Name:    "checkpoint range (9341-9401)",
			Args:    []string{"get_ledger_range_from_times", "-s", "2015-10-01T06:20:00+00:00", "-e", "2015-10-01T06:25:00+00:00", "-o", GotTestDir(t, "checkpoint_range2.txt")},
			Golden:  "checkpoint_range2.golden",
			WantErr: nil,
		},
		{
			Name:    "infinite loop; checkpoint range (14558-14606)",
			Args:    []string{"get_ledger_range_from_times", "-s", "2015-10-01T13:35:00+00:00", "-e", "2015-10-01T13:40:00+00:00", "-o", GotTestDir(t, "checkpoint_range3.txt")},
			Golden:  "checkpoint_range3.golden",
			WantErr: nil,
		},
		{
			Name:    "early checkpoint range (3-3)",
			Args:    []string{"get_ledger_range_from_times", "-s", "2015-09-30T16:50:00+00:00", "-e", "2015-09-30T16:55:00+00:00", "-o", GotTestDir(t, "early_checkpoint_range.txt")},
			Golden:  "early_checkpoint_range.golden",
			WantErr: nil,
		},
	}

	for _, test := range tests {
		RunCLITest(t, test, "testdata/ranges/", "", false)
	}
}
