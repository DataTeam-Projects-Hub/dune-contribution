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
), combined AS (
  SELECT
    date,   
    wallet,
    'initiator' AS source
  FROM initiators
  UNION ALL
  SELECT
    date,
    wallet,
    'recipient' AS source
  FROM recipients
), daily_unique AS (
  SELECT
    date,
    wallet,
    source
  FROM combined
  GROUP BY
    date,
    wallet,
    source
)
SELECT
  date,
  COUNT(DISTINCT wallet) AS unique_wallets_7d,
  COUNT(DISTINCT CASE WHEN source = 'initiator' THEN wallet END) AS unique_initiators_7d,
  COUNT(DISTINCT CASE WHEN source = 'recipient' THEN wallet END) AS unique_recipients_7d
FROM (
  SELECT
    d1.date,
    d2.wallet,
    d2.source
  FROM (
    SELECT DISTINCT
      date
    FROM daily_unique
  ) AS d1
  JOIN daily_unique AS d2
    ON d2.date BETWEEN d1.date - INTERVAL '6' day AND d1.date
) AS windowed
GROUP BY
  date
ORDER BY
  date