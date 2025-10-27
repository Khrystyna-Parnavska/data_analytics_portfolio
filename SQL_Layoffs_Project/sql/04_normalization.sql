INSERT IGNORE INTO industries(ind_name)
SELECT DISTINCT TRIM(industry)
FROM layoffs_features
ORDER BY 1; 
SELECT *
FROM industries;

INSERT IGNORE INTO countries(country_name)
SELECT DISTINCT TRIM(country)
FROM layoffs_features
ORDER BY 1; 
SELECT *
FROM countries;

INSERT IGNORE INTO locations(loc_name)
SELECT DISTINCT TRIM(location)
FROM layoffs_features
ORDER BY 1; 
SELECT *
FROM locations;

INSERT IGNORE INTO stages(stage_name)
SELECT DISTINCT TRIM(stage)
FROM layoffs_features
ORDER BY 1; 
SELECT *
FROM stages;

DROP TABLE IF EXISTS unique_companies;
CREATE TEMPORARY TABLE unique_companies AS
SELECT company, industry, location, country
FROM layoffs_features
GROUP BY 1,2,3,4;

SELECT u.company, 
	l.loc_id, 
	i.ind_id, 
    c.country_id
FROM unique_companies u
JOIN locations l ON u.location = l.loc_name
JOIN industries i ON u.industry = i.ind_name
JOIN countries c ON u.country = c.country_name
ORDER BY 1;

INSERT INTO companies (company_name, location_id, industry_id, country_id)
SELECT 
    u.company, 
    l.loc_id, 
    i.ind_id, 
    c.country_id
FROM unique_companies u
	JOIN locations l 
		ON u.location = l.loc_name
	JOIN industries i 
		ON u.industry = i.ind_name
	JOIN countries c 
		ON u.country = c.country_name
ORDER BY 1;

SELECT * FROM demo_db.layoffs;

INSERT INTO layoffs (
    company_id,
    total_laid_off,
    percentage_laid_off,
    `date`,
    stage_id,
    funds_raised_millions
)
SELECT
    c.company_id,
    lf.total_laid_off,
    lf.percentage_laid_off,
    lf.date,
    s.stage_id,
    lf.funds_raised_millions
FROM layoffs_features lf
	JOIN companies c
		ON lf.company  = c.company_name
		AND lf.location = (SELECT loc_name FROM locations WHERE loc_id = c.location_id)
		AND lf.country  = (SELECT country_name FROM countries WHERE country_id = c.country_id)
		AND lf.industry = (SELECT ind_name FROM industries WHERE ind_id = c.industry_id)
	JOIN stages s
	ON lf.stage = s.stage_name;
    
SELECT *
FROM layoffs;
