package input

import (
	"fmt"
	"time"

	"github.com/stellar/stellar-etl/internal/utils"

	"github.com/stellar/go/historyarchive"
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
	Client     historyarchive.ArchiveInterface
	BeginPoint graphPoint
	EndPoint   graphPoint
}

const avgCloseTime = time.Second * 5 // average time to close a stellar ledger

// GetLedgerRange calculates the ledger range that spans the provided date range
func GetLedgerRange(startTime, endTime time.Time, isTest bool, isFuture bool) (int64, int64, error) {
	startTime = startTime.UTC()
	endTime = endTime.UTC()
	commonFlagValues := utils.CommonFlagValues{
		IsTest:   isTest,
		IsFuture: isFuture,
	}
	env := utils.GetEnvironmentDetails(commonFlagValues)

	if startTime.After(endTime) {
		return 0, 0, fmt.Errorf("start time must be less than or equal to the end time")
	}

	graph, err := createNewGraph(env.ArchiveURLs)
	if err != nil {
		return 0, 0, err
	}

	err = graph.limitLedgerRange(&startTime, &endTime)
	if err != nil {
		return 0, 0, err
	}

	// Ledger sequence 2 is the start ledger because the genesis ledger (ledger 1), has a close time of 0 in Unix time.
	// The second ledger has a valid close time that matches with the network start time.
	startLedger, err := graph.findLedgerForDate(2, startTime, map[int64]struct{}{})
	if err != nil {
		return 0, 0, err
	}

	endLedger, err := graph.findLedgerForDate(2, endTime, map[int64]struct{}{})
	if err != nil {
		return 0, 0, err
	}

	return startLedger, endLedger, nil
}

// createNewGraph makes a new graph with the endpoints equal to the network's endpoints
func createNewGraph(archiveURLs []string) (graph, error) {
	graph := graph{}
	archive, err := utils.CreateHistoryArchiveClient(archiveURLs)
	if err != nil {
		return graph, err
	}

	graph.Client = archive

	secondLedgerPoint, err := graph.getGraphPoint(2) // the second ledger has a real close time, unlike the 1970s close time of the genesis ledger
	if err != nil {
		return graph, err
	}

	graph.BeginPoint = secondLedgerPoint

	root, err := graph.Client.GetRootHAS()
	if err != nil {
		return graph, err
	}

	latestPoint, err := graph.getGraphPoint(int64(root.CurrentLedger))
	if err != nil {
		return graph, err
	}

	graph.EndPoint = latestPoint
	return graph, nil
}

func (g graph) findLedgerForTimeBinary(targetTime time.Time, start, end graphPoint) (int64, error) {
	if end.Seq >= 2 {
		middleLedger := start.Seq + (end.Seq-start.Seq)/2
		middleTime, err := g.getGraphPoint(middleLedger)
		if err != nil {
			return 0, err
		}

		// check if middle element is the one to choose
		if middleLedger > 1 {
			prevLedger := middleLedger - 1
			prevTime, err := g.getGraphPoint(prevLedger)
			if err != nil {
				return 0, err
			}

			if prevTime.CloseTime.Unix() < targetTime.Unix() && middleTime.CloseTime.Unix() >= targetTime.Unix() {
				return middleLedger, nil
			}
		}

		if middleTime.CloseTime.Unix() > targetTime.Unix() {
			newEnd, err := g.getGraphPoint(middleLedger - 1)
			if err != nil {
				return 0, err
			}

			return g.findLedgerForTimeBinary(targetTime, start, newEnd)
		}

		newStart, err := g.getGraphPoint(middleLedger + 1)
		if err != nil {
			return 0, err
		}

		return g.findLedgerForTimeBinary(targetTime, newStart, end)
	}

	return 0, fmt.Errorf("unable to find ledger with close time %v: ", targetTime)
}

// findLedgerForDate recursively searches for the ledger that was closed on or directly after targetTime
func (g graph) findLedgerForDate(currentLedger int64, targetTime time.Time, seenLedgers map[int64]struct{}) (int64, error) {
	seenLedgers[currentLedger] = struct{}{}

	currentPoint, err := g.getGraphPoint(currentLedger)
	if err != nil {
		return 0, err
	}

	if currentLedger > 1 {
		prevLedger := currentLedger - 1
		prevTime, err := g.getGraphPoint(prevLedger)
		if err != nil {
			return 0, err
		}

		if prevTime.CloseTime.Unix() < targetTime.Unix() && currentPoint.CloseTime.Unix() >= targetTime.Unix() {
			return currentLedger, nil
		}
	}

	timeDiff := targetTime.Sub(currentPoint.CloseTime).Seconds()
	ledgerOffset := int64(timeDiff / avgCloseTime.Seconds())
	if ledgerOffset == 0 {
		if timeDiff > 0 {
			ledgerOffset = 1
		} else {
			ledgerOffset = -1
		}
	}

	newLedger := currentLedger + ledgerOffset

	if newLedger > g.EndPoint.Seq {
		newLedger = g.EndPoint.Seq
	} else if newLedger < g.BeginPoint.Seq {
		// since we started with BeginPoint, returning to it would create an infinite cycle;
		newLedger = g.BeginPoint.Seq + 1
	}

	// if we have already seen this ledger, it means the algorithm is trapped in a cycle; use binary search instead (slower but will find the ledger)
	if _, exists := seenLedgers[newLedger]; exists {
		// since we have already calculated the current point, we can use it as the upper or lower bound.
		// This way, the binary search doesn't have to search the entire space
		if ledgerOffset > 0 {
			return g.findLedgerForTimeBinary(targetTime, currentPoint, g.EndPoint)
		}

		return g.findLedgerForTimeBinary(targetTime, g.BeginPoint, currentPoint)
	}

	return g.findLedgerForDate(newLedger, targetTime, seenLedgers)
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
	ledger, err := g.Client.GetLedgerHeader(uint32(sequence))
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
