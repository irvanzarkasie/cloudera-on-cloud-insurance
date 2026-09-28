-- Cloudera Data Warehouse (Hive Virtual Warehouse)
-- Run in Hue / CDW SQL editor connected to your VW.
-- Database must match CDE Iceberg catalog (adjust if your catalog name differs).

USE holuser01_insurance_analytics;

-- Report 1: Executive summary by state (for BI dashboard)
CREATE OR REPLACE VIEW vw_executive_claims_by_state AS
SELECT
  state,
  SUM(claim_count) AS total_claims,
  ROUND(SUM(total_claim_amount), 2) AS total_claim_amount,
  ROUND(AVG(avg_fraud_risk_score), 4) AS avg_fraud_risk_score,
  SUM(high_risk_customer_claims) AS high_risk_claims,
  SUM(denied_claims) AS denied_claims
FROM gold_claims_kpi_by_state
GROUP BY state;

-- Report 2: High-risk watchlist for fraud analysts
CREATE OR REPLACE VIEW vw_high_risk_claims_report AS
SELECT
  claim_id,
  customer_id,
  state,
  policy_type,
  claim_type,
  claim_amount,
  claim_status,
  fraud_risk_score,
  claim_date
FROM gold_high_risk_watchlist
ORDER BY claim_amount DESC;

-- Quick validation queries
-- SELECT * FROM vw_executive_claims_by_state ORDER BY total_claim_amount DESC LIMIT 10;
-- SELECT COUNT(*) FROM vw_high_risk_claims_report;
