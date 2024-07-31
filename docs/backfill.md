# How to Backfill history_transactions table using tx_meta (xdr)

In cases of new column requests/addressing bugs, airflow backfill jobs are needed to be kicked off. This process is both time and money consuming.

This document outlies methods to extract required field from transaction envelope (tx_meta). This documents takes an example of `fee_account_muxed` field. However, it can be extended to other fields as well.

We will use [js-stellar-base](https://github.com/stellar/js-stellar-base) library to parse the XDRs and then use Javascript UDF in Bigquery to apply the transformation to the dataset.

Referred medium article: [Using NPM Library in Google BigQuery UDF](https://medium.com/analytics-vidhya/using-npm-library-in-google-bigquery-udf-8aef01b868f4)

# Setting up JS UDF in Bigquery

```
git clone https://github.com/stellar/js-stellar-base.git
cd js-stellar-base
yarn
yarn build:prod
```

Above will create following files in `js-stellar-base/dist/` directory:

- stellar-base.min.js

Copy above files in a GCS bucket.

# Writing JS Function

Following is example to extract FeeAccountMuxed:

```javascript
    let tx_meta = "AAAABQAAAQAAABYMYQ4r9W/uB9X6q6VU6feQhS2kQoRy9CjvwtYXdPRSih2hZeSSAAAAAAAAAZAAAAACAAAAAJwLL0Ul/CyRZdXuenmdXrzVyX9X56m4kYPYmgppVIj8AAAAZAAF9PwAAAABAAAAAAAAAAAAAAABAAAAAQAAAQAAAFj7+8N85JwLL0Ul/CyRZdXuenmdXrzVyX9X56m4kYPYmgppVIj8AAAAAAAAAADN5igtu93OKhkj2NrSHuPEJktU+0gJ0LiNavJirLAmRwAAAAAF9eEAAAAAAAAAAAFpVIj8AAAAQElnt70S4sGicHyhsN1S29DEREZ7i2HU96+8DfyshlFLCoQudDIxThnVEg2KQDrW61R19M7Ms9IAsznURc5y3wIAAAAAAAAAAaFl5JIAAABAIf9/ecA3id1mbHzJ2S9W5bRVqrjQr/c2+jHEuDNZevt3LDVSc+DmRMYie0eQ+vE7B3D+fRPb9yFzpfx4meTfBg==";

    let txe = StellarBase.xdr.TransactionEnvelope.fromXDR(tx_meta, 'base64');
    let tx = txe.feeBump();
    let sourceAccount = StellarBase.encodeMuxedAccountToAddress(tx.tx().feeSource());
    console.log(sourceAccount)
```

Above will print `MBX64B6V7KV2KVHJ66IIKLNEIKCHF5BI57BNMF3U6RJIUHNBMXSJEAAACYGGCDRL6UFO2`, which is the `fee_account_muxed` value

# Wrapping JS function as UDF

```
CREATE TEMP FUNCTION getFeeBumpAccountIfExists(tx_meta STRING)
RETURNS STRING
LANGUAGE js
OPTIONS (
  library=["gs://stellar-test-js-udf/stellar-base.min.js"] -- path to js library in GCS
)
AS r"""
    return StellarBase.encodeMuxedAccountToAddress(
        StellarBase.xdr.TransactionEnvelope.fromXDR(tx_meta, 'base64')
          .feeBump()
          .tx()
          .feeSource()
    );
""";

WITH fee_bump_transactions AS
  (
    SELECT batch_run_date, transaction_hash, tx_envelope as tx_meta FROM `test_crypto_stellar.history_transactions`
    WHERE
     batch_run_date BETWEEN DATETIME("2024-07-01") AND DATETIME_ADD("2024-07-20", INTERVAL 1 MONTH)
    and inner_transaction_hash is not null -- filter in fee bump transactions
  ),
  calculated_fee_account as (
    SELECT batch_run_date, transaction_hash, getFeeBumpAccountIfExists(tx_meta) as fee_account
    FROM fee_bump_transactions
  ),
  calculated_fee_muxed_account as (
    SELECT batch_run_date, transaction_hash, fee_account FROM calculated_fee_account
    WHERE fee_account LIKE 'M%' -- muxed accounts
  )
  SELECT batch_run_date, transaction_hash, fee_account as fee_account_muxed FROM calculated_fee_muxed_account
```

Sample output for above JS UDF.
Row | transaction_hash | fee_account
-- | -- | --
1 | f5f5b0aaf758896ef8c5b4807f41c77d15c11977eecf2b0e4769d777324a2d11 |MCBD54KAHHA4AK4DOZWOSX5O5OZ4OI54N24QITDSFLPD7EG2WY2AMAAACYGGCDRL6UBUA
2 | a9e49dff6202663633b83f3645fbf8c2cfeb915db99b2b884a86791b9f8eae2f | MBX64B6V7KV2KVHJ66IIKLNEIKCHF5BI57BNMF3U6RJIUHNBMXSJEAAACYGGCDRL6UFO2
3 | 00dba50c8689477e6990103338a0eb326725e07a7b7ff187359abf11c23c582a| MC5BEU3DCIMHOHRQDVDAPEPZGMBBALPJ3IQY23VTXC3454SQMNWVSAAACYGGCDRL6UX42
4 | 2e1c53a9fe1d48ddc493febe467178994e669e3eebf3a4cca646b3cb666616de|MAMYAUW45TC54C3QORQP7OOFYKOXCJTXOG2WIV5LP2HDMR67MWP6IAAACYGGCDRL6VCZM
