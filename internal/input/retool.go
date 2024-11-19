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

func GetRetoolData(client *apiclient.APIClient) ([]utils.RetoolEntityDataTransformInput, error) {
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
	retoolDataSlice := []utils.RetoolEntityDataTransformInput{}

	for i, item := range resultSlice {
		if itemMap, ok := item.(map[string]interface{}); ok {
			var resp utils.RetoolEntityDataTransformInput
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
