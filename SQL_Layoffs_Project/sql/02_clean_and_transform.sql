SELECT *
FROM layoffs_staging;

DESCRIBE layoffs_staging;

SELECT COUNT(*) 
FROM layoffs_staging;

## duplicates

WITH duplicate_layoffs as (
SELECT *,
ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, date, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging)

SELECT *
FROM duplicate_layoffs
WHERE row_num  > 1;

DROP TABLE IF EXISTS layoffs_staging_1;
CREATE TABLE layoffs_staging_1 AS
SELECT DISTINCT *
FROM layoffs_staging;

SELECT COUNT(*) 
FROM layoffs_staging_1;

## fixing  data inconsistencies

UPDATE layoffs_staging_1
SET company = TRIM(company);

SELECT distinct industry
FROM layoffs_staging_1
order by 1;

SELECT *
FROM layoffs_staging_1
WHERE industry LIKE 'Crypto%';

UPDATE layoffs_staging_1
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

SELECT DISTINCT location
FROM layoffs_staging_1
ORDER BY 1;

SELECT DISTINCT country
FROM layoffs_staging_1
ORDER BY 1;

UPDATE layoffs_staging_1
SET country = 'United States'
WHERE country LIKE 'United States%';

SELECT `date`, STR_TO_DATE(`date`, '%m/%d/%Y')
FROM layoffs_staging_1;

UPDATE layoffs_staging_1
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

SELECT *
FROM layoffs_staging_1
WHERE `date` IS NULL;

UPDATE layoffs_staging_1
SET `date` = '2023-02-14' # from internet
WHERE `date` IS NULL;

ALTER TABLE layoffs_staging_1
MODIFY `date` DATE;

## handling missing data

SELECT *
FROM layoffs_staging_1
WHERE industry IS NULL
OR industry = '';

SELECT *
FROM layoffs_staging_1
WHERE company = 'Airbnb';


UPDATE layoffs_staging_1
SET industry = return_industry(company)
WHERE industry IS NULL OR industry = '';

## fixing the rest
UPDATE layoffs_staging_1
SET industry = 'Other'
WHERE industry IS NULL;

UPDATE layoffs_staging_1
SET stage = 'Unknown'
WHERE stage IS NULL;

SELECT
    MAX(CHAR_LENGTH(company))   AS max_company_len,
    MAX(CHAR_LENGTH(industry))  AS max_industry_len,
    MAX(CHAR_LENGTH(country))   AS max_country_len,
    MAX(CHAR_LENGTH(location))  AS max_location_len,
    MAX(CHAR_LENGTH(stage))     AS max_stage_len
FROM layoffs_staging_1;

UPDATE layoffs_staging_1
SET funds_raised_millions = NULL
WHERE funds_raised_millions LIKE '%NULL%';

UPDATE layoffs_staging_1
SET percentage_laid_off = ROUND(percentage_laid_off, 2);

SELECT 'Max Total Laid Off Length' AS Column_Name,
		total_laid_off AS Value
FROM layoffs_staging_1
WHERE CHAR_LENGTH(total_laid_off) = (SELECT MAX(CHAR_LENGTH(total_laid_off)) FROM layoffs_staging_1)
UNION ALL
SELECT 'Max Funds Raised Length' AS Column_Name,
		funds_raised_millions AS Value
FROM layoffs_staging_1
WHERE CHAR_LENGTH(funds_raised_millions) = (SELECT MAX(CHAR_LENGTH(funds_raised_millions)) FROM layoffs_staging_1)
ORDER BY Column_Name DESC;




ALTER TABLE layoffs_staging_1
MODIFY COLUMN total_laid_off SMALLINT UNSIGNED,
MODIFY COLUMN percentage_laid_off DECIMAL(5,2),
MODIFY COLUMN funds_raised_millions DECIMAL(10,4);

DESCRIBE layoffs_staging_1;

SELECT COUNT(*) 
FROM (SELECT country, AVG(total_laid_off) AS avg_total_laid_off FROM layoffs_staging_1 GROUP BY 1) AS agg_laid_offs
WHERE avg_total_laid_off IS NULL;

SELECT COUNT(*) 
FROM (SELECT industry, AVG(total_laid_off) AS avg_total_laid_off FROM layoffs_staging_1 GROUP BY 1) AS agg_laid_offs
WHERE avg_total_laid_off IS NULL;

SELECT COUNT(*) 
FROM (SELECT industry, country, AVG(total_laid_off) AS avg_total_laid_off FROM layoffs_staging_1 GROUP BY 1,2) AS agg_laid_offs
WHERE avg_total_laid_off IS NULL;

SELECT COUNT(*) 
FROM layoffs_staging_1
WHERE total_laid_off IS NULL;

WITH avg_ind_country_laid_offs AS(
SELECT 
industry,
country,
stage,
AVG(total_laid_off) AS avg_total_laid_off
FROM layoffs_staging_1
GROUP BY 1, 2, 3)

SELECT *
FROM layoffs_staging_1 i
INNER JOIN avg_ind_country_laid_offs a
ON i.country = a.country AND i.industry = a.industry AND i.stage = a.stage
WHERE total_laid_off IS NULL;

-- total_laid_off
-- round 1 imputation
CALL ImputeMissingValuesWThreeColls(
    'layoffs_staging_1',
    'total_laid_off',
    'industry',
    'country',
    'stage'
);
 
-- round 2 imputation
CALL ImputeMissingValuesWTwoColls(
    'layoffs_staging_1',
    'total_laid_off',
    'industry',
    'country'
);

SELECT COUNT(*)
FROM layoffs_staging_1
WHERE total_laid_off IS NULL;


-- percentage_laid_off
-- round 1 imputation
CALL ImputeMissingValuesWThreeColls(
    'layoffs_staging_1',
    'percentage_laid_off',
    'industry',
    'country',
    'stage'
);

-- round 2 imputation
CALL ImputeMissingValuesWTwoColls(
    'layoffs_staging_1',
    'percentage_laid_off',
    'industry',
    'country'
);
 
SELECT COUNT(*)
FROM layoffs_staging_1
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

DELETE
FROM layoffs_staging_1
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

DROP TABLE IF EXISTS layoffs_features;
CREATE TABLE layoffs_features AS
SELECT *
FROM layoffs_staging_1;

SELECT MAX((total_laid_off * 100) / NULLIF(percentage_laid_off, 0)) 
FROM layoffs_staging_1;

ALTER TABLE layoffs_features
ADD COLUMN total_employees INT UNSIGNED; # MIGHT BE A BAD DESIGION AS PERSENTAGE IS FOR THAT EXACT LAY OFF AT THAT MOMENT

UPDATE layoffs_features
SET total_employees = (total_laid_off * 100) / NULLIF(percentage_laid_off, 0);

select count(*)
from layoffs_features
where total_employees is NULL;

-- total_employees
-- round 1 imputation
CALL ImputeMissingValuesWThreeColls(
    'layoffs_features',
    'total_employees',
    'industry',
    'country',
    'stage'
);

-- round 2 imputation
CALL ImputeMissingValuesWTwoColls(
    'layoffs_features',
    'total_employees',
    'industry',
    'country'
);