-- Always select one cumulative valuation snapshot.

SELECT *
FROM auto_insurance_gold.portfolio_performance
WHERE as_of_date = '2026-01-10';

SELECT
    region,
    COUNT(*) AS policy_count,
    SUM(earned_premium) AS earned_premium,
    SUM(reported_incurred_amount) AS reported_incurred,
    SUM(direct_expense) AS direct_expense,
    SUM(reported_contribution_before_ibnr) AS contribution_before_ibnr
FROM auto_insurance_gold.policy_performance
WHERE as_of_date = '2026-01-10'
GROUP BY region
ORDER BY earned_premium DESC;

SELECT
    expense_scope,
    expense_category,
    SUM(expense_amount) AS expense_amount
FROM auto_insurance_gold.expense_summary
WHERE as_of_date = '2026-01-10'
GROUP BY expense_scope, expense_category
ORDER BY expense_scope, expense_amount DESC;

SELECT
    claim_id,
    policy_id,
    incurred_amount,
    paid_amount,
    ledger_paid_amount,
    paid_reconciliation_difference,
    paid_reconciled
FROM auto_insurance_gold.claim_snapshot
WHERE as_of_date = '2026-01-10'
  AND NOT paid_reconciled
ORDER BY ABS(paid_reconciliation_difference) DESC;
