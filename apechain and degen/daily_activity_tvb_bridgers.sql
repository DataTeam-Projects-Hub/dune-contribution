-- https://dune.com/queries/5428577/8861276
WITH selected_chains AS (
  SELECT chain
  FROM unnest(split('{{chain_list}}', ',')) AS t(chain)
),

all_transfers AS (
  SELECT
    'apechain' AS chain,
    evt_block_time,
    "from",
    value
  FROM erc20_arbitrum.evt_transfer
  WHERE
    "to" = 0x1b98e4ed82ee1a91a65a38c690e2266364064d15
    AND contract_address = 0x7f9FBf9bDd3F4105C478b996B648FE6e828a1e98

  UNION ALL

  SELECT
    'degen' AS chain,
    evt_block_time,
    "from",
    value
  FROM erc20_base.evt_transfer
  WHERE
    "to" IN (
      0x4ed4E862860beD51a9570b96d89aF5E1B0Efefed,
      0xd9aAEc86B65D86f6A7B5B1b0c42FFA531710b6CA,
      0x4200000000000000000000000000000000000006
    )
    AND contract_address IN (
      0x4ed4E862860beD51a9570b96d89aF5E1B0Efefed,
      0xd9aAEc86B65D86f6A7B5B1b0c42FFA531710b6CA,
      0x4200000000000000000000000000000000000006
    )
)

SELECT
  t.chain,
  DATE_TRUNC('day', t.evt_block_time) AS date,
  COUNT(*) AS transaction_count,
  COUNT(DISTINCT t."from") AS unique_bridgers,
  SUM(t.value) / 1e18 AS total_tokens_bridged
FROM all_transfers t
JOIN selected_chains sc ON t.chain = sc.chain
GROUP BY t.chain, DATE_TRUNC('day', t.evt_block_time)
ORDER BY t.chain, DATE_TRUNC('day', t.evt_block_time);
