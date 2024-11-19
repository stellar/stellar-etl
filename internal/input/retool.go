package input

import (
	"fmt"
	"log"
	"net/url"

	"github.com/stellar/go/utils/apiclient"
	"github.com/stellar/stellar-etl/internal/utils"
)

const (
	baseUrl  = "https://xrri-vvsg-obfa.n7c.xano.io/api:glgSAjxV"
	authType = "api_key"
	apiKey   = "test-api-key"
)

// RetoolEntityDataTransformInput is a representation of the input for the TransformRetoolEntityData function
type RetoolEntityDataTransformInput struct {
	ID             int    `json:"id"`
	Name           string `json:"name"`
	Status         string `json:"status"`
	CreatedAt      int64  `json:"created_at"`
	UpdatedAt      *int64 `json:"updated_at"`
	Custodial      bool   `json:"custodial"`
	NonCustodial   bool   `json:"non_custodial"`
	HomeDomainsID  int    `json:"home_domains_id"`
	Description    string `json:"description"`
	WebsiteURL     string `json:"website_url"`
	SdpEnabled     bool   `json:"sdp_enabled"`
	SorobanEnabled bool   `json:"soroban_enabled"`
	Notes          string `json:"notes"`
	Verified       bool   `json:"verified"`
	FeeSponsor     bool   `json:"fee_sponsor"`
	AccountSponsor bool   `json:"account_sponsor"`
	Live           bool   `json:"live"`
}

func GetRetoolData(client *apiclient.APIClient) ([]RetoolEntityDataTransformInput, error) {
	if client == nil {
		client = &apiclient.APIClient{
			BaseURL:     baseUrl,
			AuthType:    authType,
			AuthHeaders: map[string]interface{}{"api_key": apiKey},
		}
	}

	reqParams := apiclient.RequestParams{
		RequestType: "GET",
		Endpoint:    "apps_details",
		QueryParams: url.Values{},
	}
	result, err := client.CallAPI(reqParams)
	if err != nil {
		return nil, fmt.Errorf("failed to call API: %w", err)
	}

	// Assert that the result is a slice of interfaces
	resultSlice, ok := result.([]interface{})
	if !ok {
		return nil, fmt.Errorf("Result is not a slice of interface")
	}
	retoolDataSlice := []RetoolEntityDataTransformInput{}

	for i, item := range resultSlice {
		if itemMap, ok := item.(map[string]interface{}); ok {
			var resp RetoolEntityDataTransformInput
			err := utils.MapToStruct(itemMap, &resp)
			if err != nil {
				log.Printf("Error converting map to struct: %v", err)
				continue
			}
			retoolDataSlice = append(retoolDataSlice, resp)
		} else {
			fmt.Printf("Item %d is not a map\n", i)
		}
	}

	return retoolDataSlice, nil
}
