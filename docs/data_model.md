# Data Model Architecture

## Raw Layer
- raw_contracts
- raw_conditions

## Dimension Layer
- dim_contract
  - 1 row per unique Contract Name
  - Derived fields:
    - region_code
    - offline_online
    - years_to_end
    - expired_lease_flag

## Fact Layer
- fact_condition
  - Payment condition lines
  - valid_from / valid_to
  - rent_yearly

- fact_contract_metrics
  - initial_rent_yearly
  - current_rent_yearly
  - rent_change_pct
  - high_rent_flag

## Analytical View
- vw_contracts_merged
  - Executive-ready reporting view

## Governance
- metadata_schema (data dictionary)
