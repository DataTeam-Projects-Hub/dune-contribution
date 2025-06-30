WITH bridge_addresses AS (
  SELECT
    blockchain,
    address
  FROM
    labels.bridge
),

selected_chains AS (
  SELECT TRIM(chain) AS chain
  FROM UNNEST(SPLIT('{{source_chain}}', ',')) AS t(chain)
),

all_transactions AS (
  SELECT
    'base' AS chain,
    DATE(block_time) AS tx_date,
    "from",
    "to"
  FROM base.transactions
  WHERE 'base' IN (SELECT chain FROM selected_chains)

  UNION ALL

  SELECT
    'arbitrum' AS chain,
    DATE(block_time) AS tx_date,
    "from",
    "to"
  FROM arbitrum.transactions
  WHERE 'arbitrum' IN (SELECT chain FROM selected_chains)

  UNION ALL

  SELECT
    'ethereum' AS chain,
    DATE(block_time) AS tx_date,
    "from",
    "to"
  FROM ethereum.transactions
  WHERE 'ethereum' IN (SELECT chain FROM selected_chains)

  UNION ALL

  SELECT
    'polygon' AS chain,
    DATE(block_time) AS tx_date,
    "from",
    "to"
  FROM polygon.transactions
  WHERE 'polygon' IN (SELECT chain FROM selected_chains)

  UNION ALL

  SELECT
    'optimism' AS chain,
    DATE(block_time) AS tx_date,
    "from",
    "to"
  FROM optimism.transactions
  WHERE 'optimism' IN (SELECT chain FROM selected_chains)

  UNION ALL

  SELECT
    'bnb' AS chain,
    DATE(block_time) AS tx_date,
    "from",
    "to"
  FROM bnb.transactions
  WHERE 'bnb' IN (SELECT chain FROM selected_chains)

  UNION ALL

  SELECT
    'fantom' AS chain,
    DATE(block_time) AS tx_date,
    "from",
    "to"
  FROM fantom.transactions
  WHERE 'fantom' IN (SELECT chain FROM selected_chains)
)

SELECT
  tx_date,
  chain,
  COUNT(DISTINCT "from") AS daily_unique_bridgers,

  -- 7-day moving average
  AVG(COUNT(DISTINCT "from")) OVER (
    PARTITION BY chain
    ORDER BY tx_date
    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
  ) AS MA_7_Days,

  -- 30-day moving average
  AVG(COUNT(DISTINCT "from")) OVER (
    PARTITION BY chain
    ORDER BY tx_date
    ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
  ) AS MA_30_Days

FROM
  all_transactions tx
JOIN
  bridge_addresses b
  ON tx.chain = b.blockchain
  AND tx."to" = b.address
GROUP BY
  tx_date,
  chain
ORDER BY
  tx_date,
  chain;
