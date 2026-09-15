# Change-management and IBNR experiments

These exercises extend the completed January 1–10 pipeline. Run each experiment on a new date or isolated prefix so the verified baseline remains reproducible.

## 1. Effective-dated policy change

### Objective

Demonstrate that a policy attribute can change prospectively while historical exposure and prior Gold snapshots retain the values known at their valuation dates.

### Proposed event contract

The sample file at `examples/policy_changes/policy_changes_2026-01-11.csv` changes `POL-0000001` from Standard to Comprehensive coverage and updates its annual premium effective January 11.

The event key is `policy_change_id`. Ordering is controlled by `source_version` and `source_updated_at`. The Silver policy-history table should retain both the original and updated effective-dated versions rather than overwriting the January 1 policy snapshot.

### Implementation steps

1. Add a `policy_changes` Bronze prefix and a Bronze-to-Silver Glue job.
2. Validate the policy exists, `source_version` increases, and `effective_date` is not earlier than the source update date's business date.
3. Publish an effective-dated Silver table with `valid_from` and `valid_to`.
4. Join each exposure day to the policy version effective on that day.
5. Run January 11 and compare the January 10 and January 11 Gold snapshots.

### Acceptance checks

- January 1–10 results do not change.
- January 11 exposure uses the new policy attributes.
- There is no overlap or gap between policy versions.
- Reprocessing January 11 is idempotent.

The current DAG does not ingest policy master changes, so this requires a new branch before it can be executed end to end.

## 2. Claim-payment schema evolution

### Objective

Add an optional `payment_method` column while showing how the pipeline protects itself from an unreviewed source schema change.

The sample at `examples/schema_evolution/claim_payments_v2_sample.csv` contains the new column. The current Silver job requires the exact v1 header and should fail before writing its partition. That failure is the expected first result.

For the compatible migration:

1. Version the Bronze contract as claim payments v2.
2. Add nullable `payment_method` validation with an allowed-value list.
3. Map older v1 records to `payment_method=NULL`.
4. Add the nullable column to the Glue Catalog table.
5. Deploy the script before sending v2 data.
6. Rerun the isolated date and verify both v1 and v2 history remain queryable.

### Acceptance checks

- An undeclared header change fails without replacing a valid Silver partition.
- V1 records remain readable after the migration.
- V2 records preserve the new column.
- Gold results remain economically unchanged because payment method is descriptive.

## 3. Bornhuetter-Ferguson IBNR

### Objective

Estimate ultimate losses and IBNR by accident month using earned premium, a reviewed expected loss ratio, and a reviewed cumulative reported-loss pattern.

For each accident month:

```text
expected ultimate loss = earned premium × expected loss ratio
expected unreported loss = expected ultimate loss × (1 - reported proportion)
BF ultimate loss = reported incurred loss + expected unreported loss
IBNR = BF ultimate loss - reported incurred loss
```

The implementation is `Scripts/silver_to_gold_ibnr_bf.py`. Its required arguments are:

| Argument | Meaning |
|---|---|
| `--PROCESS_DATE` | Valuation date |
| `--HISTORY_START_DATE` | First exposure date; must be the first of a month |
| `--EXPECTED_LOSS_RATIO` | Reviewed ELR as a decimal fraction |
| `--REPORTED_PROPORTIONS_JSON` | Consecutive development ages mapped to cumulative reported proportions |
| `--MODEL_VERSION` | Auditable assumption version |

Do not invent the ELR or development pattern from ten days of synthetic results. The short history is insufficient for a credible empirical loss-development pattern. For a demonstration, label assumptions as illustrative and keep the model result separate from the verified reported Gold metrics.

### Acceptance checks

- Every required Silver date exists.
- Claim versions resolve without ambiguity.
- Reported proportions are monotonic, bounded by zero and one, and end at one.
- One output is written per accident month, valuation date, and model version.
- Portfolio IBNR equals the sum across accident months for one valuation/model version.
- Adjusted underwriting result equals `reported_result_before_ibnr - IBNR`.
