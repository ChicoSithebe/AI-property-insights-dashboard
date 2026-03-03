# Data Model

## Raw Tables
- raw_contracts
- raw_conditions

## Dimension
- dim_contract (1 row per Contract Name)

## Facts
- fact_condition (payment lines)
- fact_contract_metrics (initial vs current rent)

## View
- vw_contracts_merged

## Metadata
- metadata_schema (data dictionary)
