WITH income_statement AS (
  SELECT 'Income Statement' AS category, 'Revenue' AS section, 'A. Transaction Fees' AS item, 'L1 Base Fee' AS metric, '2025-Q1' AS time, 851300 AS value UNION ALL
  SELECT 'Income Statement', 'Revenue', 'A. Transaction Fees', 'L1 Base Fee', '2025-Q2 Incomp.', 181560 UNION ALL
  SELECT 'Income Statement', 'Revenue', 'A. Transaction Fees', 'L2 Base Fee', '2025-Q1', 864620 UNION ALL
  SELECT 'Income Statement', 'Revenue', 'A. Transaction Fees', 'L2 Base Fee', '2025-Q2 Incomp.', 803280 UNION ALL
  SELECT 'Income Statement', 'Revenue', 'B. Timeboost', 'Timeboost', '2025-Q2 Incomp.', 192000 UNION ALL
  SELECT 'Income Statement', 'Revenue', 'C. Arbitrum Expansion Program', 'Orbit Licensing Fees', '2025-Q1', 41490 UNION ALL
  SELECT 'Income Statement', 'Cost of Revenue', 'A. Sequencer Cost', 'Blob', '2025-Q1', -481320 UNION ALL
  SELECT 'Income Statement', 'Cost of Revenue', 'A. Sequencer Cost', 'Blob', '2025-Q2 Incomp.', -56630 UNION ALL
  SELECT 'Income Statement', 'Cost of Revenue', 'A. Sequencer Cost', 'Call Data', '2025-Q1', -311070 UNION ALL
  SELECT 'Income Statement', 'Cost of Revenue', 'A. Sequencer Cost', 'Call Data', '2025-Q2 Incomp.', -95980 UNION ALL
  SELECT 'Income Statement', 'Cost of Revenue', 'B. Arbitrum Developer Guild', 'Arbitrum Developer Guild', '2025-Q1', -85430 UNION ALL
  SELECT 'Income Statement', 'Cost of Revenue', 'B. Arbitrum Developer Guild', 'Arbitrum Developer Guild', '2025-Q2 Incomp.', -54410
)

SELECT
  category,  -- Optional: not needed for Dune to group it, but adds clarity
  section,
  item,
  metric,
  time,
  value
FROM income_statement
