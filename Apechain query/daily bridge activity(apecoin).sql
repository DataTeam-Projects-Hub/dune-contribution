SELECT
  DATE_TRUNC('day', evt_block_time) AS date,
  COUNT(*) AS transaction_count,
  COUNT(DISTINCT "from") AS unique_bridgers,
  SUM(value) / 1e18 AS total_ape_bridged
FROM erc20_arbitrum.evt_transfer
WHERE
  "to" = 0x1b98e4ed82ee1a91a65a38c690e2266364064d15
  AND contract_address = 0x7f9FBf9bDd3F4105C478b996B648FE6e828a1e98
GROUP BY
  1
ORDER BY
  date;
