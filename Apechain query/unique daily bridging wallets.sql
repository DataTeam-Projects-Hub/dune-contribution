WITH initiators AS (
  SELECT
    DATE_TRUNC('day', block_time) AS date,
    "from" AS wallet
  FROM arbitrum.transactions
  WHERE
    "to" = 0x1B98e4ED82Ee1a91A65a38C690e2266364064D15
), recipients AS (
  SELECT
    DATE_TRUNC('day', evt_block_time) AS date,
    "to" AS wallet  
  FROM erc20_apechain.evt_transfer
  WHERE
    "from" = 0x0000000000000000000000000000000000000000
)
SELECT
  date,
  COUNT(DISTINCT wallet) AS unique_wallets_involved,
  COUNT(DISTINCT CASE WHEN source = 'initiator' THEN wallet END) AS unique_initiators,
  COUNT(DISTINCT CASE WHEN source = 'recipient' THEN wallet END) AS unique_recipients
FROM (
  SELECT date, wallet, 'initiator' AS source FROM initiators
  UNION ALL
  SELECT date, wallet, 'recipient' AS source FROM recipients
) AS combined
GROUP BY date
ORDER BY date;
