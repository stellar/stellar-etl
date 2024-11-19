package input

import (
	"fmt"
	"net/url"

	"github.com/stellar/go/utils/apiclient"
)

// type RetoolDataInput struct {
// 	Id         int32
// 	Name       string
// 	Addresses  []string
// 	HomeDomain string
// }

func GetRetoolData() error {
	c := &apiclient.APIClient{
		BaseURL:     "https://xrri-vvsg-obfa.n7c.xano.io/api:glgSAjxV",
		AuthType:    "api_key",
		AuthHeaders: map[string]interface{}{"api_key": "test-api-key"},
	}
	reqParams := apiclient.RequestParams{
		RequestType: "GET",
		Endpoint:    "apps_details",
		QueryParams: url.Values{},
	}
	result, err := c.CallAPI(reqParams)
	// if err != nil {
	// 	return fmt.Errorf("failed to call API: %w", err)
	// }
	fmt.Println("API Call Result:", result)
	return err
}
