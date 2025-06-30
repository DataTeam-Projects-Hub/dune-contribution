WITH bridge_addresses AS (
  SELECT
    blockchain,
    address,
    name AS protocol_name  -- bridge protocol (e.g., Wormhole, Synapse)
  FROM labels.bridge
),

selected_chains AS (
  SELECT TRIM(chain) AS chain
  FROM UNNEST(SPLIT('{{source_chain}}', ',')) AS t(chain)
),

-- Step 1: Load transactions from selected chains
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

-- Step 2: Extract inbound bridgers
inbound_bridgers AS (
  SELECT
    tx.tx_date,
    b.protocol_name,
    tx."from" AS user_address
  FROM all_transactions tx
  JOIN bridge_addresses b
    ON tx.chain = b.blockchain
   AND tx."to" = b.address
),

-- Step 3: Extract outbound bridgers
outbound_bridgers AS (
  SELECT
    tx.tx_date,
    b.protocol_name,
    tx."to" AS user_address  -- receiving address from bridge contract
  FROM all_transactions tx
  JOIN bridge_addresses b
    ON tx.chain = b.blockchain
   AND tx."from" = b.address
),

-- Step 4: Combine inbound + outbound
combined_bridgers AS (
  SELECT * FROM inbound_bridgers
  UNION ALL
  SELECT * FROM outbound_bridgers
)

-- Step 5: Count unique bridgers by protocol per day
SELECT
  tx_date,
  protocol_name,
  COUNT(DISTINCT user_address) AS total_unique_bridgers
FROM combined_bridgers
GROUP BY tx_date, protocol_name
ORDER BY tx_date, protocol_name;
