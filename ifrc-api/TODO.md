### TODO

- [X] Change all month values from YYYY-M to YYYY-MM (ie having 2015-08 when there is currently just 2015-8)

- [X] Appeals
    What are the numbers?
    Provide names

- [X] Num_of_operations_by_epidemic_type
    - Let’s be consistent in field formatting with underscoring and lowercase. ie for these values:
          - "Cholera outbreak":  cholera_outbreak
          - "Meningitis": meningitis
          - "Rift Valley fever": rift_valley_fever
          - "Viral haemorrhagic fevers": viral_haemorrhagic_fevers
          - "Viral hepatitis (A, B, C, E)": viral_hepatitis_a_b_c_e
          - "Yellow fever": yellow_fever
    - Using camelCase

- [X] latest_disaster
    - consistent naming conventions
        - rw_api_url > source_url
        - primary_type_code_rw > primary_type_code
            - basically, rm all ‘rw’s

- [X] Num_of_operations_by_crisis_type
    - All ‘count’ values lowercase

- [X] Fts
    - fundingTotals > funding_totals
    - using camelCases.

- [X] Num_reported_events
    - How come ACLED shows decimals for counts of reported events?

- [X] Use completed taxonomy table for translation

- [X] Add CLI http://click.pocoo.org/5

- [ ] Fts hpc api call use asyn, and limit the fields
