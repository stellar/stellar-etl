package input

import (
	"fmt"
	"time"

	"github.com/stellar/go/exp/ingest/ledgerbackend"
	"github.com/stellar/stellar-etl/internal/utils"
)

// graphPoint represents a single point in the graph. It includes the ledger sequence and close time (in UTC)
type graphPoint struct {
	Seq       int64
	CloseTime time.Time
}

/*
The graph struct is used to calculate ledger ranges from time ranges. It keeps track of its boundaries, and uses its backend to
retrieve new graphPoints as necessary. As the sequence number increases, so does the close time, so we can use the graph to find
sequence numbers that correspond to a given close time fairly easily.
*/
type graph struct {
	Backend    *ledgerbackend.HistoryArchiveBackend
	BeginPoint graphPoint
	EndPoint   graphPoint
}

const avgCloseTime = time.Second * 5 // average time to close a stellar ledger

// GetLedgerRange calculates the ledger range that spans the provided date range
func GetLedgerRange(startTime, endTime time.Time) (int64, int64, error) {
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

	// Ledger sequence 2 is the start ledger because the genesis ledger (ledger 1), has a close time of 0 in Unix time.
	// The second ledger has a valid close time that matches with the network start time.
	startLedger, err := graph.findLedgerForDate(2, startTime)
	if err != nil {
		return 0, 0, err
	}

	endLedger, err := graph.findLedgerForDate(2, endTime)
	if err != nil {
		return 0, 0, err
	}

	return startLedger, endLedger, nil
}

func (g graph) close() {
	g.Backend.Close()
}

// createNewGraph makes a new graph with the endpoints equal to the network's endpoints
func createNewGraph() (graph, error) {
	graph := graph{}
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

	latestPoint, err := graph.getGraphPoint(int64(latestNum))
	if err != nil {
		return graph, err
	}

	graph.EndPoint = latestPoint
	return graph, nil
}

// findLedgerForDate recursively searches for the ledger that was closed on or directly after targetTime
func (g graph) findLedgerForDate(currentLedger int64, targetTime time.Time) (int64, error) {
	currentTime, err := g.getGraphPoint(currentLedger)
	if err != nil {
		return 0, err
	}

	if currentLedger > 1 {
		prevLedger := currentLedger - 1
		prevTime, err := g.getGraphPoint(prevLedger)
		if err != nil {
			return 0, err
		}

		if prevTime.CloseTime.Unix() <= targetTime.Unix() && currentTime.CloseTime.Unix() >= targetTime.Unix() {
			return currentLedger, nil
		}
	}

	timeDiff := targetTime.Sub(currentTime.CloseTime)
	ledgerOffset := int64(timeDiff.Seconds() / avgCloseTime.Seconds())
	if ledgerOffset == 0 {
		ledgerOffset = 1
	}

	currentLedger += ledgerOffset

	if currentLedger > g.EndPoint.Seq {
		currentLedger = g.EndPoint.Seq
	} else if currentLedger < g.BeginPoint.Seq {
		// since we started with BeginPoint, returning to it would create an infinite cycle;
		currentLedger = g.BeginPoint.Seq + 1
	}

	return g.findLedgerForDate(currentLedger, targetTime)
}

// limitLedgerRange restricts start and end by setting them to be the edges of the network's range if they are outside that range
func (g graph) limitLedgerRange(start, end *time.Time) error {
	if start.Before(g.BeginPoint.CloseTime) {
		*start = g.BeginPoint.CloseTime
	} else if start.After(g.EndPoint.CloseTime) {
		*start = g.EndPoint.CloseTime
	}

	if end.After(g.EndPoint.CloseTime) {
		*end = g.EndPoint.CloseTime
	} else if end.Before(g.BeginPoint.CloseTime) {
		*end = g.BeginPoint.CloseTime
	}

	return nil
}

// getGraphPoint gets the graphPoint representation of the ledger with the provided sequence number
func (g graph) getGraphPoint(sequence int64) (graphPoint, error) {
	ok, ledger, err := g.Backend.GetLedger(uint32(sequence))
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
