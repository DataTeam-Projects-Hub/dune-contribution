with current as (
  select
    wallet_address,
    amount_raw / 1e18 as xai
  from
    balances_arbitrum.erc20_latest
  where
    token_address = '{{address}}'
    and last_updated = current_timestamp
),
stats as (
  select
    sum(xai) as xai_supply,
    count(xai) as total_holders
  from current
  where xai >= 0.000000000006
),
net_flow as (
  select
    sum(case when to_address != from_address then value / 1e18 else 0 end) 
      - sum(case when from_address != to_address then value / 1e18 else 0 end) as net_token_flow
  from erc20_arbitrum.evt_Transfer
  where token_address = '{{address}}'
)

select
  s.*,
  nf.net_token_flow
from stats s
left join net_flow nf on true
