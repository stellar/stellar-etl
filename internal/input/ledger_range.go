package input

import (
	"fmt"
	"time"

	"github.com/stellar/go/exp/ingest/ledgerbackend"
	"github.com/stellar/stellar-etl/internal/utils"
)

type graphPoint struct {
	Seq       uint32
	CloseTime time.Time
}

type grapher struct {
	Backend              *ledgerbackend.HistoryArchiveBackend
	BeginPoint, EndPoint graphPoint
}

const avgCloseTime = time.Second * 5 // average time to close a stellar ledger

// GetLedgerRange calculates the ledger range that spans the provided date range
func GetLedgerRange(startTime, endTime time.Time) (uint32, uint32, error) {
	startTime = startTime.UTC()
	endTime = endTime.UTC()

	if startTime.After(endTime) {
		return 0, 0, fmt.Errorf("start time must be less than or equal to the end time")
	}

	graph, err := createNewGraph()
	if err != nil {
		return 0, 0, err
	}

	defer graph.close()

	err = graph.limitLedgerRange(&startTime, &endTime)
	if err != nil {
		return 0, 0, err
	}
	fmt.Println(startTime, endTime)

	startLedger, err := graph.findLedgerForDate(startTime)
	if err != nil {
		return 0, 0, err
	}

	endLedger, err := graph.findLedgerForDate(endTime)
	if err != nil {
		return 0, 0, err
	}

	startCT, _ := graph.getGraphPoint(startLedger)
	endCT, _ := graph.getGraphPoint(endLedger)
	fmt.Println("original times: ", startTime, endTime)
	fmt.Println("seq nums: ", startLedger, endLedger)
	fmt.Println("close times: ", startCT, endCT)

	return startLedger, endLedger, nil
}

func (g grapher) close() {
	g.Backend.Close()
}

func createNewGraph() (grapher, error) {
	graph := grapher{}
	archive, err := utils.CreateBackend()
	if err != nil {
		return graph, err
	}

	graph.Backend = archive

	secondLedgerPoint, err := graph.getGraphPoint(2) // the second ledger has a real close time, unlike the 1970s close time of the genesis ledger
	if err != nil {
		return graph, err
	}

	graph.BeginPoint = secondLedgerPoint

	latestNum, err := graph.Backend.GetLatestLedgerSequence()
	if err != nil {
		return graph, err
	}

	latestPoint, err := graph.getGraphPoint(latestNum)
	if err != nil {
		return graph, err
	}

	graph.EndPoint = latestPoint
	return graph, nil
}

func (g grapher) recur(currentLedger uint32, targetTime time.Time) (uint32, error) {
	currentTime, err := g.getGraphPoint(currentLedger)
	if err != nil {
		return 0, err
	}

	if currentLedger > 1 {
		tempLedger := currentLedger - 1
		tempTime, err := g.getGraphPoint(tempLedger)
		if err != nil {
			return 0, err
		}

		if tempTime.CloseTime.Before(targetTime) && (currentTime.CloseTime.After(targetTime) || currentTime.CloseTime.Equal(targetTime)) {
			return currentLedger, nil
		}
	}

	//todo fix going out of range / over last ledger
	timeDiff := targetTime.Sub(currentTime.CloseTime)
	ledgerOffset := uint32(timeDiff.Seconds() / avgCloseTime.Seconds())
	if ledgerOffset == 0 {
		ledgerOffset = 1
	}

	fmt.Println("Current seq is: ", currentLedger)
	fmt.Println("Current time is: ", currentTime)
	fmt.Println("Going for: ", targetTime)
	fmt.Println("Offset is: ", ledgerOffset)
	fmt.Println("--------------")

	currentLedger += ledgerOffset

	return g.recur(currentLedger, targetTime)
}

func (g grapher) findLedgerForDate(t time.Time) (uint32, error) {
	//todo put recur code here
	timeDiff := t.Sub(g.BeginPoint.CloseTime)
	ledgerOffset := uint32(timeDiff.Seconds() / avgCloseTime.Seconds())
	currentLedger := 2 + ledgerOffset
	return g.recur(currentLedger, t)
}

func (g grapher) limitLedgerRange(start, end *time.Time) error {
	if start.Before(g.BeginPoint.CloseTime) {
		*start = g.BeginPoint.CloseTime
	}

	if end.After(g.EndPoint.CloseTime) {
		*end = g.EndPoint.CloseTime
	}

	return nil
}

func (g grapher) getGraphPoint(sequence uint32) (graphPoint, error) {
	ok, ledger, err := g.Backend.GetLedger(sequence)
	if !ok {
		return graphPoint{}, fmt.Errorf("ledger %d does not exist in history archive", sequence)
	}

	if err != nil {
		return graphPoint{}, fmt.Errorf(fmt.Sprintf("unable to get ledger %d: ", sequence), err)
	}

	closeTime, err := utils.ExtractLedgerCloseTime(ledger)
	if err != nil {
		return graphPoint{}, fmt.Errorf(fmt.Sprintf("unable to extract close time from ledger %d: ", sequence), err)
	}

	return graphPoint{
		Seq:       sequence,
		CloseTime: closeTime,
	}, nil
}
