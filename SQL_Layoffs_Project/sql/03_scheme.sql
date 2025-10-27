DROP TABLE IF EXISTS `layoffs`;
DROP TABLE IF EXISTS `companies`;
DROP TABLE IF EXISTS `locations`;
DROP TABLE IF EXISTS `industries`;
DROP TABLE IF EXISTS `stages`;
DROP TABLE IF EXISTS `countries`;

CREATE TABLE `locations`(
    `loc_id` SMALLINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    `loc_name` VARCHAR(30) NOT NULL
);
CREATE TABLE `industries`(
    `ind_id` SMALLINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    `ind_name` VARCHAR(30) NOT NULL
);
CREATE TABLE `stages`(
    `stage_id` SMALLINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    `stage_name` VARCHAR(30) NOT NULL
);
CREATE TABLE `countries`(
    `country_id` SMALLINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    `country_name` VARCHAR(30) NOT NULL
);
CREATE TABLE `companies`(
    `company_id` SMALLINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    `company_name` VARCHAR(30) NOT NULL,
    `location_id` SMALLINT UNSIGNED NULL,
    `industry_id` SMALLINT UNSIGNED NULL,
    `country_id` SMALLINT UNSIGNED NULL
);
CREATE TABLE `layoffs`(
    `layoff_id` SMALLINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    `company_id` SMALLINT UNSIGNED NULL,
    `total_laid_off` SMALLINT UNSIGNED NULL,
    `percentage_laid_off` DECIMAL(5, 2) NULL,
    `date` DATE NULL,
    `stage_id` SMALLINT UNSIGNED NULL,
    `funds_raised_millions` DECIMAL(10, 4) NULL
);
ALTER TABLE
    `companies` ADD CONSTRAINT `companies_industry_id_foreign` FOREIGN KEY(`industry_id`) REFERENCES `industries`(`ind_id`);
ALTER TABLE
    `companies` ADD CONSTRAINT `companies_location_id_foreign` FOREIGN KEY(`location_id`) REFERENCES `locations`(`loc_id`);
ALTER TABLE
    `companies` ADD CONSTRAINT `companies_country_id_foreign` FOREIGN KEY(`country_id`) REFERENCES `countries`(`country_id`);
ALTER TABLE
    `layoffs` ADD CONSTRAINT `layoffs_stage_id_foreign` FOREIGN KEY(`stage_id`) REFERENCES `stages`(`stage_id`);
ALTER TABLE
    `layoffs` ADD CONSTRAINT `layoffs_company_id_foreign` FOREIGN KEY(`company_id`) REFERENCES `companies`(`company_id`);