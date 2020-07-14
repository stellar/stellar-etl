package transform

import "time"

//LedgerOutput is a representation of a ledger that aligns with the BigQuery table history_ledger
type LedgerOutput struct {
	//sequence number of the ledger
	Sequence int32

	LedgerHash         string
	PreviousLedgerHash string
	//base 64 encoding of the ledger header
	LedgerHeader []byte

	TransactionCount           int32
	OperationCount             int
	SuccessfulTransactionCount int32
	FailedTransactionCount     int32

	//UTC timestamp
	ClosedAt time.Time

	TotalCoins      int64
	FeePool         int64
	BaseFee         int32
	BaseReserve     int32
	MaxTxSetSize    int32
	ProtocolVersion int32

	/*
		TODO: implement these three fields
			CreatedAt time.Time //timestamp of table entry creation time
			UpdatedAt time.Time //timestamp of table entry update time
			ImporterVersion int32 //version of the ingestion system
	*/
}
