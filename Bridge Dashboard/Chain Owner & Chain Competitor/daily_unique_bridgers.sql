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
  SELECT 'base' AS chain, DATE(block_time) AS tx_date, "from", "to"
  FROM base.transactions
  WHERE 'base' IN (SELECT chain FROM selected_chains)

  UNION ALL
  SELECT 'arbitrum' AS chain, DATE(block_time) AS tx_date, "from", "to"
  FROM arbitrum.transactions
  WHERE 'arbitrum' IN (SELECT chain FROM selected_chains)

  UNION ALL
  SELECT 'ethereum' AS chain, DATE(block_time) AS tx_date, "from", "to"
  FROM ethereum.transactions
  WHERE 'ethereum' IN (SELECT chain FROM selected_chains)

  UNION ALL
  SELECT 'polygon' AS chain, DATE(block_time) AS tx_date, "from", "to"
  FROM polygon.transactions
  WHERE 'polygon' IN (SELECT chain FROM selected_chains)

  UNION ALL
  SELECT 'optimism' AS chain, DATE(block_time) AS tx_date, "from", "to"
  FROM optimism.transactions
  WHERE 'optimism' IN (SELECT chain FROM selected_chains)

  UNION ALL
  SELECT 'bnb' AS chain, DATE(block_time) AS tx_date, "from", "to"
  FROM bnb.transactions
  WHERE 'bnb' IN (SELECT chain FROM selected_chains)

  UNION ALL
  SELECT 'fantom' AS chain, DATE(block_time) AS tx_date, "from", "to"
  FROM fantom.transactions
  WHERE 'fantom' IN (SELECT chain FROM selected_chains)
),

inbound AS (
  SELECT
    tx_date,
    chain,
    COUNT(DISTINCT "from") AS inbound_unique_bridgers
  FROM all_transactions tx
  JOIN bridge_addresses b
    ON tx.chain = b.blockchain
   AND tx."to" = b.address
  GROUP BY tx_date, chain
),

outbound AS (
  SELECT
    tx_date,
    chain,
    COUNT(DISTINCT "from") AS outbound_unique_bridgers
  FROM all_transactions tx
  JOIN bridge_addresses b
    ON tx.chain = b.blockchain
   AND tx."from" = b.address
  GROUP BY tx_date, chain
),

combined AS (
  SELECT
    COALESCE(i.tx_date, o.tx_date) AS tx_date,
    COALESCE(i.chain, o.chain) AS chain,
    COALESCE(i.inbound_unique_bridgers, 0) AS inbound_unique_bridgers,
    COALESCE(o.outbound_unique_bridgers, 0) AS outbound_unique_bridgers
  FROM inbound i
  FULL OUTER JOIN outbound o
    ON i.tx_date = o.tx_date AND i.chain = o.chain
)

SELECT
  tx_date,
  chain,
  inbound_unique_bridgers,
  outbound_unique_bridgers,
  
  -- Total unique bridgers (not deduped across directions)
  (inbound_unique_bridgers + outbound_unique_bridgers) AS total_bridgers,

    (inbound_unique_bridgers - outbound_unique_bridgers) AS net_unique_bridgers


FROM combined
ORDER BY tx_date, chain;
