WITH bridge_addresses AS (
  SELECT
    blockchain AS source_chain,
    address,
    name AS protocol_name
  FROM labels.bridge
),

selected_chains AS (
  SELECT TRIM(chain) AS chain
  FROM UNNEST(SPLIT('{{source_chain}}', ',')) AS t(chain)
),

-- Step 1: Get all transactions interacting with bridge contracts
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

-- Step 2: Identify outbound transactions from a bridge contract
outbound_bridge_txs AS (
  SELECT
    tx_date,
    t.chain AS source_chain,
    b.protocol_name,
    t."to" AS user_address
  FROM all_transactions t
  JOIN bridge_addresses b
    ON t.chain = b.source_chain
   AND t."from" = b.address
),

-- Step 3: Infer destination chain (basic heuristic: any chain ≠ source)
destination_candidates AS (
  SELECT DISTINCT blockchain AS destination_chain
  FROM labels.bridge
),

-- Step 4: Pair each source chain with all possible destinations ≠ source
-- (Used to rank where users go via protocols from each source)
cross_chain_paths AS (
  SELECT
    o.tx_date,
    o.protocol_name,
    o.source_chain,
    d.destination_chain,
    o.user_address
  FROM outbound_bridge_txs o
  JOIN destination_candidates d
    ON o.source_chain != d.destination_chain
)

-- Step 5: Count frequency of inferred destinations per protocol
SELECT
  tx_date,
  protocol_name,
  source_chain,
  destination_chain,
  COUNT(DISTINCT user_address) AS bridgers_to_destination
FROM cross_chain_paths
GROUP BY tx_date, protocol_name, source_chain, destination_chain
ORDER BY tx_date, protocol_name, bridgers_to_destination DESC;
