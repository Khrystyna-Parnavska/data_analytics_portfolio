USE demo_db;
DROP TABLE IF EXISTS `layoffs_staging`;
CREATE TABLE `layoffs_staging`(
	`company` TEXT,
    `location` TEXT,
    `industry` TEXT,
    `total_laid_off` TEXT,
    `percentage_laid_off` TEXT,
    `date` TEXT,
    `stage` TEXT,
    `country` TEXT,
    `funds_raised_millions` TEXT
);

LOAD DATA INFILE '/var/lib/mysql-files/layoffs.csv'
INTO TABLE layoffs_staging
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

