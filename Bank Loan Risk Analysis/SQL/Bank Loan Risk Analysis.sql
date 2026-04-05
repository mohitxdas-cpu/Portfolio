
SET GLOBAL local_infile = 1;

CREATE DATABASE IF NOT EXISTS bank_loan_analytics;
USE bank_loan_analytics;


CREATE TABLE IF NOT EXISTS loans (
    borrower_id   VARCHAR(20)   PRIMARY KEY,
    credit_score  SMALLINT      NOT NULL,
    annual_income DECIMAL(12,2) NOT NULL,
    dti_ratio     DECIMAL(5,4)  NOT NULL,
    loan_amount   DECIMAL(12,2) NOT NULL,
    loan_purpose  VARCHAR(50)   NOT NULL,
    loan_status   VARCHAR(20)   NOT NULL,  
    risk_score    DECIMAL(5,1)  NOT NULL,


    CONSTRAINT chk_credit_score CHECK (credit_score BETWEEN 300 AND 850),
    CONSTRAINT chk_dti          CHECK (dti_ratio BETWEEN 0 AND 1),
    CONSTRAINT chk_loan_amt     CHECK (loan_amount > 0),
    CONSTRAINT chk_income       CHECK (annual_income > 0)
);


CREATE INDEX idx_loan_status   ON loans(loan_status);
CREATE INDEX idx_credit_score  ON loans(credit_score);
CREATE INDEX idx_loan_purpose  ON loans(loan_purpose);


LOAD DATA LOCAL INFILE 'C:/Users/Mohit/Downloads/bank_loan_data.csv'
INTO TABLE loans
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(borrower_id, credit_score, annual_income, dti_ratio, loan_amount,
 loan_purpose, loan_status, risk_score);

SELECT * FROM loans LIMIT 10;


SELECT
    COUNT(*) AS total_records,

    SUM(CASE WHEN credit_score IS NULL THEN 1 ELSE 0 END) AS null_credit_score,
    SUM(CASE WHEN dti_ratio IS NULL THEN 1 ELSE 0 END)    AS null_dti,

    SUM(CASE WHEN loan_amount <= 0 THEN 1 ELSE 0 END)     AS invalid_loan_amt,
    SUM(CASE WHEN annual_income <= 0 THEN 1 ELSE 0 END)   AS invalid_income,

    SUM(CASE WHEN credit_score NOT BETWEEN 300 AND 850 THEN 1 ELSE 0 END)
        AS out_of_range_credit_score

FROM loans;


SELECT
    COUNT(*) AS total_borrowers,

    ROUND(SUM(loan_amount) / 10000000.0, 2) AS total_exposure_cr,

    ROUND(AVG(credit_score), 0)  AS avg_credit_score,
    ROUND(AVG(dti_ratio) * 100, 1) AS avg_dti_pct,

    SUM(CASE WHEN loan_status = 'Default' THEN 1 ELSE 0 END) AS total_defaults,

    ROUND(
        100.0 * SUM(CASE WHEN loan_status = 'Default' THEN 1 ELSE 0 END)
        / NULLIF(COUNT(*), 0),
        1
    ) AS default_rate_pct,

    ROUND(AVG(annual_income), 0) AS avg_annual_income

FROM loans
WHERE credit_score BETWEEN 300 AND 850
  AND dti_ratio BETWEEN 0 AND 1
  AND loan_amount > 0
  AND annual_income > 0;


SELECT
    CASE
        WHEN credit_score < 580               THEN 'Poor (<580)'
        WHEN credit_score BETWEEN 580 AND 669 THEN 'Fair (580-669)'
        WHEN credit_score BETWEEN 670 AND 739 THEN 'Good (670-739)'
        ELSE                                       'Excellent (740+)'
    END AS score_band,

    COUNT(*) AS total_borrowers,

    SUM(CASE WHEN loan_status = 'Default' THEN 1 ELSE 0 END) AS defaults,

    ROUND(
        100.0 * SUM(CASE WHEN loan_status = 'Default' THEN 1 ELSE 0 END)
        / NULLIF(COUNT(*), 0),
        1
    ) AS default_rate_pct,

    ROUND(AVG(dti_ratio) * 100, 1) AS avg_dti_pct,
    ROUND(AVG(loan_amount), 0)     AS avg_loan_amount

FROM loans
GROUP BY score_band
ORDER BY default_rate_pct DESC;


SELECT
    loan_purpose,

    COUNT(*) AS loan_count,

    ROUND(SUM(loan_amount) / 10000000.0, 1) AS exposure_cr,

    ROUND(AVG(dti_ratio) * 100, 1) AS avg_dti_pct,
    ROUND(AVG(credit_score), 0)    AS avg_credit_score,

    ROUND(
        100.0 * SUM(CASE WHEN loan_status = 'Default' THEN 1 ELSE 0 END)
        / NULLIF(COUNT(*), 0),
        1
    ) AS default_rate_pct

FROM loans
GROUP BY loan_purpose
ORDER BY exposure_cr DESC;


SELECT
    borrower_id,
    credit_score,
    ROUND(annual_income, 0)   AS annual_income,
    ROUND(dti_ratio * 100, 1) AS dti_pct,
    loan_amount,
    loan_purpose,
    loan_status,

    ROUND(
        (1 - (credit_score - 300) / 550.0) * 40
        + dti_ratio * 35,
    1) AS computed_risk_score

FROM loans
WHERE credit_score < 580
   OR dti_ratio > 0.50
ORDER BY computed_risk_score DESC
LIMIT 20;



WITH band_summary AS (
    SELECT
        CASE
            WHEN credit_score < 580               THEN 'Poor (<580)'
            WHEN credit_score BETWEEN 580 AND 669 THEN 'Fair (580-669)'
            WHEN credit_score BETWEEN 670 AND 739 THEN 'Good (670-739)'
            ELSE                                       'Excellent (740+)'
        END AS score_band,

        COUNT(*) AS total_borrowers,

        SUM(CASE WHEN loan_status = 'Default' THEN 1 ELSE 0 END) AS defaults,

        ROUND(
            100.0 * SUM(CASE WHEN loan_status = 'Default' THEN 1 ELSE 0 END)
            / NULLIF(COUNT(*), 0),
            1
        ) AS default_rate_pct

    FROM loans
    GROUP BY score_band
)

SELECT
    score_band,
    total_borrowers,
    defaults,
    default_rate_pct,

    ROUND(
        100.0 * total_borrowers / SUM(total_borrowers) OVER (),
        1
    ) AS pct_of_portfolio,

    SUM(defaults) OVER (ORDER BY default_rate_pct DESC)
        AS cumulative_defaults

FROM band_summary
ORDER BY default_rate_pct DESC;



SELECT
    CASE
        WHEN annual_income < 300000  THEN 'Low (<3L)'
        WHEN annual_income < 600000  THEN 'Lower-Mid (3L-6L)'
        WHEN annual_income < 1000000 THEN 'Mid (6L-10L)'
        WHEN annual_income < 1500000 THEN 'Upper-Mid (10L-15L)'
        ELSE                              'High (15L+)'
    END AS income_bracket,

    COUNT(*) AS borrowers,

    ROUND(AVG(credit_score), 0) AS avg_credit_score,

    ROUND(
        100.0 * SUM(CASE WHEN loan_status = 'Default' THEN 1 ELSE 0 END)
        / NULLIF(COUNT(*), 0),
        1
    ) AS default_rate_pct

FROM loans
GROUP BY income_bracket
ORDER BY
    CASE income_bracket
        WHEN 'Low (<3L)'           THEN 1
        WHEN 'Lower-Mid (3L-6L)'   THEN 2
        WHEN 'Mid (6L-10L)'        THEN 3
        WHEN 'Upper-Mid (10L-15L)' THEN 4
        ELSE 5
    END;