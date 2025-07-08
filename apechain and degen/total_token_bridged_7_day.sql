-- https://dune.com/queries/5428568/8861296
WITH selected_chains AS (
  SELECT chain
  FROM unnest(split('{{chain_list}}', ',')) AS t(chain)
),

-- Raw transfers
all_transfers AS (
  SELECT
    'apechain' AS chain,
    evt_block_time,
    "from",
    value,
    'Apecoin' AS token_name
  FROM erc20_arbitrum.evt_transfer
  WHERE
    "to" = 0x1b98e4ed82ee1a91a65a38c690e2266364064d15
    AND contract_address = 0x7f9FBf9bDd3F4105C478b996B648FE6e828a1e98

  UNION ALL

  SELECT
    'degen' AS chain,
    evt_block_time,
    "from",
    value,
    CASE contract_address
      WHEN 0x4ed4E862860beD51a9570b96d89aF5E1B0Efefed THEN 'DEGEN'
      WHEN 0xd9aAEc86B65D86f6A7B5B1b0c42FFA531710b6CA THEN 'USDC'
      WHEN 0x4200000000000000000000000000000000000006 THEN 'WETH'
      ELSE 'Unknown'
    END AS token_name
  FROM erc20_base.evt_transfer
  WHERE
    "to" IN (
      0x4ed4E862860beD51a9570b96d89aF5E1B0Efefed,
      0xd9aAEc86B65D86f6A7B5B1b0c42FFA531710b6CA,
      0x4200000000000000000000000000000000000006
    )
    AND contract_address IN (
      0x4ed4E862860beD51a9570b96d89aF5E1B0Efefed,  -- DEGEN
      0xd9aAEc86B65D86f6A7B5B1b0c42FFA531710b6CA,  -- USDC
      0x4200000000000000000000000000000000000006   -- WETH
    )
),

-- Distinct dates to build time windows
all_dates AS (
  SELECT DISTINCT DATE_TRUNC('day', evt_block_time) AS date
  FROM all_transfers
),

-- Join each date to its 7-day window
rolling_7d AS (
  SELECT
    d.date,
    t.chain,
    t.token_name,
    t.value,
    t.evt_block_time
  FROM all_dates d
  JOIN all_transfers t
    ON DATE_TRUNC('day', t.evt_block_time) BETWEEN d.date - INTERVAL '6' DAY AND d.date
)

SELECT
  r.chain,
  r.token_name,
  r.date,
  SUM(r.value) / 1e18 AS total_tokens_bridged_7d
FROM rolling_7d r
JOIN selected_chains sc
  ON r.chain = sc.chain
GROUP BY r.chain, r.token_name, r.date
ORDER BY r.chain, r.token_name, r.date;
