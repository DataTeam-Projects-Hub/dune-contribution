SELECT
  DATE_TRUNC('day', evt_block_time) AS date,
  COUNT(*) AS total_transactions,
  COUNT(DISTINCT "from") AS unique_bridgers,
  SUM(value) / 1e18 AS total_tokens_bridged
FROM erc20_base.evt_transfer
WHERE
  "to" IN (
    0x4ed4E862860beD51a9570b96d89aF5E1B0Efefed,  -- DEGEN
    0xd9aAEc86B65D86f6A7B5B1b0c42FFA531710b6CA,  -- USDC
    0x4200000000000000000000000000000000000006   -- WETH
  )
  AND contract_address IN (
    0x4ed4E862860beD51a9570b96d89aF5E1B0Efefed,
    0xd9aAEc86B65D86f6A7B5B1b0c42FFA531710b6CA,
    0x4200000000000000000000000000000000000006
  )
GROUP BY
  1
ORDER BY
  date;
