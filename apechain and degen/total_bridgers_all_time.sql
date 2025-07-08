-- https://dune.com/queries/5398426/8860730
WITH selected_chains AS (
  SELECT chain
  FROM unnest(split('{{chain_list}}', ',')) AS t(chain)
),

initiators AS (
  SELECT
    'apechain' AS chain,
    DATE_TRUNC('day', block_time) AS date,
    "from" AS wallet
  FROM arbitrum.transactions
  WHERE
    "to" = 0x1B98e4ED82Ee1a91A65a38C690e2266364064D15

  UNION ALL

  SELECT
    'degen' AS chain,
    DATE_TRUNC('day', block_time) AS date,
    "from" AS wallet
  FROM base.transactions
  WHERE
    "to" IN (
      0x4ed4E862860beD51a9570b96d89aF5E1B0Efefed,
      0xd9aAEc86B65D86f6A7B5B1b0c42FFA531710b6CA,
      0x4200000000000000000000000000000000000006
    )
),

recipients AS (
  SELECT
    'apechain' AS chain,
    DATE_TRUNC('day', evt_block_time) AS date,
    "to" AS wallet
  FROM erc20_apechain.evt_transfer
  WHERE
    "from" = 0x0000000000000000000000000000000000000000

  UNION ALL

  SELECT
    'degen' AS chain,
    DATE_TRUNC('day', evt_block_time) AS date,
    "to" AS wallet
  FROM erc20_degen.evt_transfer
  WHERE
    "from" = 0x0000000000000000000000000000000000000000
)

SELECT
  combined.chain,
  combined.date,
  COUNT(DISTINCT combined.wallet) AS unique_bridgers,
  COUNT(DISTINCT CASE WHEN combined.source = 'initiator' THEN combined.wallet END) AS unique_initiators,
  COUNT(DISTINCT CASE WHEN combined.source = 'recipient' THEN combined.wallet END) AS unique_recipients
FROM (
  SELECT chain, date, wallet, 'initiator' AS source FROM initiators
  UNION ALL
  SELECT chain, date, wallet, 'recipient' AS source FROM recipients
) AS combined
JOIN selected_chains sc ON combined.chain = sc.chain
GROUP BY combined.chain, combined.date
ORDER BY combined.chain, combined.date;
