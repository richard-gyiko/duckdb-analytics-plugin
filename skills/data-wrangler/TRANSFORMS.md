# Advanced Transformations

DuckDB-specific SQL patterns for data transformation.

## Contents
- [PIVOT - Rows to Columns](#pivot---rows-to-columns)
- [UNPIVOT - Columns to Rows](#unpivot---columns-to-rows)
- [Sampling](#sampling)
- [Dynamic Column Selection](#dynamic-column-selection)
- [Window Functions](#window-functions)
- [Date/Time Transformations](#datetime-transformations)

---

## PIVOT - Rows to Columns

Turn row values into columns for reporting.

### Basic PIVOT

```sql
-- Convert quarters to columns
PIVOT sales 
ON quarter 
USING SUM(revenue) 
GROUP BY region
```

**Before:**
| region | quarter | revenue |
|--------|---------|---------|
| North  | Q1      | 100     |
| North  | Q2      | 150     |
| South  | Q1      | 80      |

**After:**
| region | Q1  | Q2  |
|--------|-----|-----|
| North  | 100 | 150 |
| South  | 80  | NULL|

### PIVOT with Multiple Aggregations

```sql
PIVOT sales 
ON quarter 
USING SUM(revenue) AS total, COUNT(*) AS count 
GROUP BY region
```

### Dynamic PIVOT (from subquery)

```sql
PIVOT sales 
ON (SELECT DISTINCT quarter FROM sales WHERE year = 2024) 
USING SUM(revenue)
```

---

## UNPIVOT - Columns to Rows

Turn columns into rows for normalization.

### Basic UNPIVOT

```sql
UNPIVOT quarterly_data 
ON q1, q2, q3, q4 
INTO NAME quarter VALUE amount
```

**Before:**
| region | q1  | q2  | q3  | q4  |
|--------|-----|-----|-----|-----|
| North  | 100 | 150 | 200 | 180 |

**After:**
| region | quarter | amount |
|--------|---------|--------|
| North  | q1      | 100    |
| North  | q2      | 150    |
| North  | q3      | 200    |
| North  | q4      | 180    |

### UNPIVOT with Column Pattern

```sql
-- Unpivot all columns starting with 'metric_'
UNPIVOT data 
ON COLUMNS('metric_.*') 
INTO NAME metric VALUE value
```

---

## Sampling

Get random subsets for testing, validation, or ML training.

### Fixed Row Count

```sql
-- Exactly 1000 random rows
SELECT * FROM large_table 
USING SAMPLE 1000 ROWS
```

### Percentage Sampling

```sql
-- Approximately 10% of rows
SELECT * FROM large_table 
USING SAMPLE 10%
```

### Reproducible Sampling

Use `REPEATABLE` for deterministic results:

```sql
-- Same random sample every time
SELECT * FROM data 
USING SAMPLE 10% REPEATABLE(42)
```

### Sampling Methods

| Method | Speed | Accuracy | Use Case |
|--------|-------|----------|----------|
| `reservoir` | Slower | Exact count | Small samples, exact row counts |
| `bernoulli` | Fast | Approximate | Row-level sampling |
| `system` | Fastest | Approximate | Large datasets, quick estimates |

```sql
-- Fast approximate sampling for huge datasets
SELECT * FROM huge_table 
USING SAMPLE 1% (system)

-- Exact count for ML validation set
SELECT * FROM training_data 
USING SAMPLE 1000 (reservoir) REPEATABLE(42)
```

### TABLESAMPLE (alternative syntax)

```sql
-- SQL-standard syntax
SELECT * FROM large_table 
TABLESAMPLE SYSTEM(5)  -- ~5% of blocks
```

---

## Dynamic Column Selection

Flexible column selection without hardcoding names.

### EXCLUDE - Remove Specific Columns

```sql
-- All columns except sensitive ones
SELECT * EXCLUDE (password, ssn, credit_card) 
FROM users
```

### REPLACE - Transform Specific Columns

```sql
-- Mask email, lowercase name
SELECT * REPLACE (
    REGEXP_REPLACE(email, '.+@', '***@') AS email,
    LOWER(name) AS name
) 
FROM users
```

### COLUMNS - Pattern-Based Selection

```sql
-- Select all amount columns
SELECT COLUMNS('.*_amount$') 
FROM transactions

-- Sum all numeric columns matching pattern
SELECT COLUMNS('metric_.*').SUM() 
FROM metrics

-- Apply function to matching columns
SELECT region, COLUMNS('q[1-4]') * 1.1 AS adjusted 
FROM quarterly_sales
```

### Combining Dynamic Selection

```sql
-- All columns, excluding sensitive, with transformations
SELECT * 
    EXCLUDE (internal_id, raw_data)
    REPLACE (ROUND(amount, 2) AS amount)
FROM payments
```

---

## Window Functions

Analytics over partitions of data.

### Running Totals

```sql
SELECT 
    date,
    amount,
    SUM(amount) OVER (ORDER BY date) AS running_total,
    SUM(amount) OVER (
        PARTITION BY category 
        ORDER BY date
    ) AS category_running_total
FROM transactions
```

### Ranking

```sql
SELECT 
    name,
    score,
    ROW_NUMBER() OVER (ORDER BY score DESC) AS rank,
    DENSE_RANK() OVER (ORDER BY score DESC) AS dense_rank,
    NTILE(4) OVER (ORDER BY score DESC) AS quartile
FROM players
```

### Moving Averages

```sql
SELECT 
    date,
    value,
    AVG(value) OVER (
        ORDER BY date 
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS moving_avg_7d
FROM daily_metrics
```

### Lead/Lag

```sql
SELECT 
    date,
    value,
    LAG(value, 1) OVER (ORDER BY date) AS prev_value,
    LEAD(value, 1) OVER (ORDER BY date) AS next_value,
    value - LAG(value) OVER (ORDER BY date) AS day_over_day
FROM metrics
```

---

## Date/Time Transformations

Common date manipulation patterns.

### Extracting Components

```sql
SELECT 
    timestamp,
    YEAR(timestamp) AS year,
    MONTH(timestamp) AS month,
    DAYOFWEEK(timestamp) AS dow,  -- 0=Sunday
    HOUR(timestamp) AS hour,
    DATE_TRUNC('month', timestamp) AS month_start
FROM events
```

### Date Arithmetic

```sql
SELECT 
    date,
    date + INTERVAL 7 DAY AS week_later,
    date - INTERVAL 1 MONTH AS month_ago,
    DATEDIFF('day', start_date, end_date) AS days_between,
    AGE(birth_date) AS age
FROM dates
```

### Formatting

```sql
SELECT 
    STRFTIME(timestamp, '%Y-%m-%d') AS iso_date,
    STRFTIME(timestamp, '%B %d, %Y') AS readable_date,
    STRFTIME(timestamp, '%H:%M:%S') AS time_only
FROM events
```

### Timezone Handling

```sql
SELECT 
    ts AT TIME ZONE 'America/New_York' AS eastern,
    ts AT TIME ZONE 'UTC' AS utc
FROM events
```

---

## String Transformations

### Regex Operations

```sql
SELECT 
    REGEXP_EXTRACT(email, '@(.+)$', 1) AS domain,
    REGEXP_REPLACE(phone, '[^0-9]', '') AS digits_only,
    REGEXP_MATCHES(text, 'pattern')
FROM contacts
```

### String Functions

```sql
SELECT 
    LOWER(name) AS lowercase,
    UPPER(name) AS uppercase,
    TRIM(name) AS trimmed,
    LEFT(name, 5) AS first_5,
    SPLIT_PART(full_name, ' ', 1) AS first_name,
    CONCAT(first, ' ', last) AS full_name,
    LENGTH(text) AS char_count
FROM strings
```

---

## Aggregation Patterns

### FILTER Clause

```sql
SELECT 
    COUNT(*) AS total,
    COUNT(*) FILTER (status = 'active') AS active,
    SUM(amount) FILTER (type = 'sale') AS sales_total,
    AVG(amount) FILTER (amount > 0) AS avg_positive
FROM transactions
```

### GROUP BY with ROLLUP/CUBE

```sql
-- Subtotals and grand total
SELECT region, product, SUM(sales)
FROM sales
GROUP BY ROLLUP (region, product)

-- All combinations
SELECT region, product, SUM(sales)
FROM sales
GROUP BY CUBE (region, product)
```

### QUALIFY - Filter Window Results

```sql
-- Top 3 products per category
SELECT category, product, sales
FROM products
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY category 
    ORDER BY sales DESC
) <= 3
```
