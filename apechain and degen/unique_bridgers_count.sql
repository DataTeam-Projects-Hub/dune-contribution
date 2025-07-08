-- https://dune.com/queries/5429723/8862596?chain_list_e15077=degen
WITH selected_chains AS (
  SELECT chain
  FROM unnest(split('{{chain_list}}', ',')) AS t(chain)
),

all_transfers AS (
  SELECT
    'apechain' AS chain,
    evt_block_time,
    "from"
  FROM erc20_arbitrum.evt_transfer
  WHERE
    "to" = 0x1b98e4ed82ee1a91a65a38c690e2266364064D15
    AND contract_address = 0x7f9FBf9bDd3F4105C478b996B648FE6e828a1e98

  UNION ALL

  SELECT
    'degen' AS chain,
    evt_block_time,
    "from"
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
)

SELECT
  t.chain,
  COUNT(DISTINCT t."from") AS total_unique_bridgers,
  COUNT(DISTINCT CASE WHEN t.evt_block_time >= CURRENT_TIMESTAMP - INTERVAL '30' DAY THEN t."from" END) AS unique_bridgers_30d,
  COUNT(DISTINCT CASE WHEN t.evt_block_time >= CURRENT_TIMESTAMP - INTERVAL '7' DAY THEN t."from" END) AS unique_bridgers_7d,
  COUNT(DISTINCT CASE WHEN t.evt_block_time >= CURRENT_TIMESTAMP - INTERVAL '1' DAY THEN t."from" END) AS unique_bridgers_24h
FROM all_transfers t
JOIN selected_chains sc ON t.chain = sc.chain
GROUP BY t.chain
ORDER BY t.chain;
