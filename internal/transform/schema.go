package transform

import "time"

// LedgerOutput is a representation of a ledger that aligns with the BigQuery table history_ledgers
type LedgerOutput struct {
	Sequence                   uint32    `json:"sequence"` // sequence number of the ledger
	LedgerHash                 string    `json:"ledger_hash"`
	PreviousLedgerHash         string    `json:"previous_ledger_hash"`
	LedgerHeader               string    `json:"ledger_header"` // base 64 encoding of the ledger header
	TransactionCount           int32     `json:"transaction_count"`
	OperationCount             int32     `json:"operation_count"` // counts only operations that were a part of successful transactions
	SuccessfulTransactionCount int32     `json:"successful_transaction_count"`
	FailedTransactionCount     int32     `json:"failed_transaction_count"`
	TxSetOperationCount        string    `json:"tx_set_operation_count"` // counts all operations, even those that are part of failed transactions
	ClosedAt                   time.Time `json:"closed_at"`              // UTC timestamp
	TotalCoins                 int64     `json:"total_coins"`
	FeePool                    int64     `json:"fee_pool"`
	BaseFee                    uint32    `json:"base_fee"`
	BaseReserve                uint32    `json:"base_reserve"`
	MaxTxSetSize               uint32    `json:"max_tx_set_size"`
	ProtocolVersion            uint32    `json:"protocol_version"`
	LedgerID                   int64     `json:"id"`
}

// TransactionOutput is a representation of a transaction that aligns with the BigQuery table history_transactions
type TransactionOutput struct {
	TransactionHash  string    `json:"transaction_hash"`
	LedgerSequence   uint32    `json:"ledger_sequence"`
	ApplicationOrder uint32    `json:"application_order"`
	Account          string    `json:"account"`
	AccountSequence  int64     `json:"account_sequence"`
	MaxFee           uint32    `json:"max_fee"`
	FeeCharged       int64     `json:"fee_charged"`
	OperationCount   int32     `json:"operation_count"`
	CreatedAt        time.Time `json:"created_at"`
	MemoType         string    `json:"memo_type"`
	Memo             string    `json:"memo"`
	TimeBounds       string    `json:"time_bounds"`
	Successful       bool      `json:"successful"`
	TransactionID    int64     `json:"id"`
}

// AccountOutput is a representation of an account that aligns with the BigQuery table accounts
type AccountOutput struct {
	AccountID            string `json:"account_id"` // account address
	Balance              int64  `json:"balance"`
	BuyingLiabilities    int64  `json:"buying_liabilities"`
	SellingLiabilities   int64  `json:"selling_liabilities"`
	SequenceNumber       int64  `json:"sequence_number"`
	NumSubentries        uint32 `json:"num_subentries"`
	InflationDestination string `json:"inflation_destination"`
	Flags                uint32 `json:"flags"`
	HomeDomain           string `json:"home_domain"`
	MasterWeight         int32  `json:"master_weight"`
	ThresholdLow         int32  `json:"threshold_low"`
	ThresholdMedium      int32  `json:"threshold_medium"`
	ThresholdHigh        int32  `json:"threshold_high"`
	LastModifiedLedger   uint32 `json:"last_modified_ledger"`
	Deleted              bool   `json:"deleted"`
}

// OperationOutput is a representation of an operation that aligns with the BigQuery table history_operations
type OperationOutput struct {
	SourceAccount    string  `json:"source_account"`
	Type             int32   `json:"type"`
	ApplicationOrder int32   `json:"application_order"`
	OperationDetails Details `json:"details"`
	TransactionID    int64   `json:"transaction_id"`
	OperationID      int64   `json:"id"`
}

// Details is a struct that provides additional information about operations in a way that aligns with the details struct in the BigQuery table history_operations
type Details struct {
	Account            string   `json:"account"`
	Amount             float64  `json:"amount"`
	AssetCode          string   `json:"asset_code"`
	AssetIssuer        string   `json:"asset_issuer"`
	AssetType          string   `json:"asset_type"`
	Authorize          bool     `json:"authorize"`
	BuyingAssetCode    string   `json:"buying_asset_code"`
	BuyingAssetIssuer  string   `json:"buying_asset_issuer"`
	BuyingAssetType    string   `json:"buying_asset_type"`
	From               string   `json:"from"`
	Funder             string   `json:"funder"`
	HighThreshold      uint32   `json:"high_threshold"`
	HomeDomain         string   `json:"home_domain"`
	InflationDest      string   `json:"inflation_dest"`
	Into               string   `json:"into"`
	Limit              float64  `json:"limit"`
	LowThreshold       uint32   `json:"low_threshold"`
	MasterKeyWeight    uint32   `json:"master_key_weight"`
	MedThreshold       uint32   `json:"med_threshold"`
	Name               string   `json:"name"`
	OfferID            int64    `json:"offer_id"`
	Path               []Path   `json:"path"`
	Price              float64  `json:"price"`
	PriceR             Price    `json:"price_r"`
	SellingAssetCode   string   `json:"selling_asset_code"`
	SellingAssetIssuer string   `json:"selling_asset_issuer"`
	SellingAssetType   string   `json:"selling_asset_type"`
	SetFlags           []int32  `json:"set_flags"`
	SetFlagsString     []string `json:"set_flags_s"`
	SignerKey          string   `json:"signer_key"`
	SignerWeight       uint32   `json:"signer_weight"`
	SourceAmount       float64  `json:"source_amount"`
	SourceAssetCode    string   `json:"source_asset_code"`
	SourceAssetIssuer  string   `json:"source_asset_issuer"`
	SourceAssetType    string   `json:"source_asset_type"`
	SourceMax          float64  `json:"source_max"`
	StartingBalance    float64  `json:"starting_balance"`
	To                 string   `json:"to"`
	Trustee            string   `json:"trustee"`
	Trustor            string   `json:"trustor"`
	Value              string   `json:"value"` // base64 encoding of bytes for operations that manage data
	ClearFlags         []int32  `json:"clear_flags"`
	ClearFlagsString   []string `json:"clear_flags_s"`
	DestinationMin     string   `json:"destination_min"`
	BumpTo             string   `json:"bump_to"`
}

// Price represents the price of an asset as a fraction
type Price struct {
	Numerator   int32 `json:"n"`
	Denominator int32 `json:"d"`
}

// Path is a representation of an asset without an ID that forms part of a path in a path payment
type Path struct {
	AssetCode   string `json:"asset_code"`
	AssetIssuer string `json:"asset_issuer"`
	AssetType   string `json:"asset_type"`
}

// AssetOutput is a representation of an asset that aligns with the BigQuery table history_assets
type AssetOutput struct {
	AssetCode   string `json:"asset_code"`
	AssetIssuer string `json:"asset_issuer"`
	AssetType   string `json:"asset_type"`
	AssetID     uint64 `json:"id"`
}

// TrustlineOutput is a representation of a trustline that aligns with the BigQuery table trust_lines
type TrustlineOutput struct {
	LedgerKey          string `json:"ledger_key"`
	AccountID          string `json:"account_id"`
	AssetCode          string `json:"asset_code"`
	AssetIssuer        string `json:"asset_issuer"`
	AssetType          int32  `json:"asset_type"`
	Balance            int64  `json:"balance"`
	TrustlineLimit     int64  `json:"trust_line_limit"`
	BuyingLiabilities  int64  `json:"buying_liabilities"`
	SellingLiabilities int64  `json:"selling_liabilities"`
	Flags              uint32 `json:"flags"`
	LastModifiedLedger uint32 `json:"last_modified_ledger"`
	Deleted            bool   `json:"deleted"`
}

// OfferOutput is a representation of an offer that aligns with the BigQuery table offers
type OfferOutput struct {
	SellerID           string  `json:"seller_id"` // Account address of the seller
	OfferID            int64   `json:"offer_id"`
	SellingAsset       uint64  `json:"selling_asset"`
	BuyingAsset        uint64  `json:"buying_asset"`
	Amount             int64   `json:"amount"`
	PriceN             int32   `json:"pricen"`
	PriceD             int32   `json:"priced"`
	Price              float64 `json:"price"`
	Flags              uint32  `json:"flags"`
	LastModifiedLedger uint32  `json:"last_modified_ledger"`
	Deleted            bool    `json:"deleted"`
}

// TradeOutput is a representation of a trade that aligns with the BigQuery table history_trades
type TradeOutput struct {
	Order                 int32     `json:"order"`
	LedgerClosedAt        time.Time `json:"ledger_closed_at"`
	OfferID               int64     `json:"offer_id"`
	BaseAccountAddress    string    `json:"base_account_address"`
	BaseAssetCode         string    `json:"base_asset_code"`
	BaseAssetIssuer       string    `json:"base_asset_issuer"`
	BaseAssetType         string    `json:"base_asset_type"`
	BaseAmount            int64     `json:"base_amount"`
	CounterAccountAddress string    `json:"counter_account_address"`
	CounterAssetCode      string    `json:"counter_asset_code"`
	CounterAssetIssuer    string    `json:"counter_asset_issuer"`
	CounterAssetType      string    `json:"counter_asset_type"`
	CounterAmount         int64     `json:"counter_amount"`
	BaseIsSeller          bool      `json:"base_is_seller"`
	PriceN                int64     `json:"price_n"`
	PriceD                int64     `json:"price_d"`
	BaseOfferID           int64     `json:"base_offer_id"`
	CounterOfferID        int64     `json:"counter_offer_id"`
	HistoryOperationID    int64     `json:"history_operation_id"`
}

//DimAccount is a representation of an account that aligns with the BigQuery table dim_accounts
type DimAccount struct {
	ID      uint64 `json:"account_id"`
	Address string `json:"address"`
}

// DimOffer is a representation of an account that aligns with the BigQuery table dim_offers
type DimOffer struct {
	HorizonID     int64   `json:"horizon_offer_id"`
	DimOfferID    uint64  `json:"dim_offer_id"`
	MarketID      uint64  `json:"market_id"`
	MakerID       uint64  `json:"maker_id"`
	Action        string  `json:"action"`
	BaseAmount    int64   `json:"base_amount"`
	CounterAmount float64 `json:"counter_amount"`
	Price         float64 `json:"price"`
}

// FactOfferEvent is a representation of an offer event that aligns with the BigQuery table fact_offer_events
type FactOfferEvent struct {
	LedgerSeq       uint32 `json:"ledger_id"`
	OfferInstanceID uint64 `json:"offer_instance_id"`
}

// DimMarket is a representation of an account that aligns with the BigQuery table dim_markets
type DimMarket struct {
	ID            uint64 `json:"market_id"`
	BaseCode      string `json:"base_code"`
	BaseIssuer    string `json:"base_issuer"`
	CounterCode   string `json:"counter_code"`
	CounterIssuer string `json:"counter_issuer"`
}

// NormalizedOfferOutput ties together the information for dim_markets, dim_offers, dim_accounts, and fact_offer-events
type NormalizedOfferOutput struct {
	Market  DimMarket
	Offer   DimOffer
	Account DimAccount
	Event   FactOfferEvent
}
