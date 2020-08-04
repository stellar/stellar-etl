package input

import (
	"fmt"
	"testing"

	"github.com/stellar/stellar-etl/internal/transform"
	"github.com/stretchr/testify/assert"
)

func TestAcc(t *testing.T) {
	accounts, err := GetAccounts(24088895, 24088895, -1)
	assert.NoError(t, err)

	for _, acc := range accounts {
		transformed, err := transform.TransformAccount(acc)
		assert.NoError(t, err)
		fmt.Println(transformed)
	}
}
