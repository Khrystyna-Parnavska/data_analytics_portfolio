DELIMITER $$
DROP PROCEDURE IF EXISTS ImputeMissingValuesWThreeColls;
-- Stored procedure to impute missing numeric values based on averages of grouping columns.
-- This is designed to perform a single round of imputation based on three grouping columns.
CREATE PROCEDURE ImputeMissingValuesWThreeColls(
    IN tableName VARCHAR(255),          -- The name of the table to update (e.g., 'layoffs_staging_1')
    IN targetColumn VARCHAR(255),       -- The column with missing values (e.g., 'total_laid_off')
    IN groupCol1 VARCHAR(255),          -- First grouping column (e.g., 'industry')
    IN groupCol2 VARCHAR(255),          -- Second grouping column (e.g., 'country')
    IN groupCol3 VARCHAR(255)           -- Third grouping column (e.g., 'stage')
)
BEGIN
    -- 1. Create a dynamic table name for the temporary aggregated averages
    -- This ensures we can run the procedure multiple times without conflicts.
    SET @tempTableName = CONCAT('temp_avg_impute_', REPLACE(UUID(), '-', '_'));

    -- 2. Build the query to calculate the averages and store them in a temporary table.
    -- This replicates the logic from your initial CTE (Common Table Expression).
    SET @createTempSql = CONCAT('
        CREATE TEMPORARY TABLE ', @tempTableName, ' AS
        SELECT
            ', groupCol1, ',
            ', groupCol2, ',
            ', groupCol3, ',
            AVG(CASE WHEN ', targetColumn, ' IS NOT NULL THEN ', targetColumn, ' END) AS avg_imputed_value
        FROM
            ', tableName, '
        GROUP BY 1, 2, 3
        HAVING avg_imputed_value IS NOT NULL;
    ');

    PREPARE stmt1 FROM @createTempSql;
    EXECUTE stmt1;
    DEALLOCATE PREPARE stmt1;

    -- 3. Build the query to update the original table (imputation).
    -- It joins the staging table with the new temporary averages table.
    SET @updateSql = CONCAT('
        UPDATE
            ', tableName, ' AS t1
        INNER JOIN
            ', @tempTableName, ' AS t2
            ON t1.', groupCol1, ' = t2.', groupCol1, '
            AND t1.', groupCol2, ' = t2.', groupCol2, '
            AND t1.', groupCol3, ' = t2.', groupCol3, '
        SET
            t1.', targetColumn, ' = ROUND(t2.avg_imputed_value)
        WHERE
            t1.', targetColumn, ' IS NULL
            AND t2.avg_imputed_value IS NOT NULL;
    ');

    PREPARE stmt2 FROM @updateSql;
    EXECUTE stmt2;
    DEALLOCATE PREPARE stmt2;

    -- 4. Clean up the temporary table.
    SET @dropTempSql = CONCAT('DROP TEMPORARY TABLE IF EXISTS ', @tempTableName, ';');
    PREPARE stmt3 FROM @dropTempSql;
    EXECUTE stmt3;
    DEALLOCATE PREPARE stmt3;

    -- 5. Report the result (Imputed rows and remaining NULLs)
    SELECT
        ROW_COUNT() AS imputed_rows_count,
        (SELECT COUNT(*) FROM layoffs_staging_1 WHERE total_laid_off IS NULL) AS remaining_nulls_count;

END $$

DELIMITER ;


DELIMITER $$
DROP PROCEDURE IF EXISTS ImputeMissingValuesWTwoColls;

CREATE PROCEDURE ImputeMissingValuesWTwoColls(
    IN tableName VARCHAR(255),
    IN targetColumn VARCHAR(255),
    IN groupCol1 VARCHAR(255),
    IN groupCol2 VARCHAR(255)
)
BEGIN
    SET @tempTableName = CONCAT('temp_avg_impute_r2_', REPLACE(UUID(), '-', '_'));

    -- Calculate averages based on two columns
    SET @createTempSql = CONCAT('
        CREATE TEMPORARY TABLE ', @tempTableName, ' AS
        SELECT
            ', groupCol1, ',
            ', groupCol2, ',
            AVG(CASE WHEN ', targetColumn, ' IS NOT NULL THEN ', targetColumn, ' END) AS avg_imputed_value
        FROM
            ', tableName, '
        GROUP BY 1, 2
        HAVING avg_imputed_value IS NOT NULL;
    ');

    PREPARE stmt1 FROM @createTempSql;
    EXECUTE stmt1;
    DEALLOCATE PREPARE stmt1;

    -- Update the original table for remaining NULLs
    SET @updateSql = CONCAT('
        UPDATE
            ', tableName, ' AS t1
        INNER JOIN
            ', @tempTableName, ' AS t2
            ON t1.', groupCol1, ' = t2.', groupCol1, '
            AND t1.', groupCol2, ' = t2.', groupCol2, '
        SET
            t1.', targetColumn, ' = ROUND(t2.avg_imputed_value)
        WHERE
            t1.', targetColumn, ' IS NULL
            AND t2.avg_imputed_value IS NOT NULL;
    ');

    PREPARE stmt2 FROM @updateSql;
    EXECUTE stmt2;
    DEALLOCATE PREPARE stmt2;

    SET @dropTempSql = CONCAT('DROP TEMPORARY TABLE IF EXISTS ', @tempTableName, ';');
    PREPARE stmt3 FROM @dropTempSql;
    EXECUTE stmt3;
    DEALLOCATE PREPARE stmt3;

    -- Report the result for Round 2
    SELECT
        ROW_COUNT() AS imputed_rows_count_r2,
        (SELECT COUNT(*) FROM layoffs_staging_1 WHERE total_laid_off IS NULL) AS final_remaining_nulls;

END $$

DELIMITER ;


## function for finding indusrty to fill in where it missing for particular company
DELIMITER $$
DROP FUNCTION IF EXISTS return_industry;

CREATE FUNCTION return_industry(company_name TEXT)
RETURNS TEXT
DETERMINISTIC
BEGIN
	DECLARE industry_name VARCHAR(50);
    
	SELECT industry
    INTO industry_name
	FROM layoffs_staging_1
	WHERE (industry IS NOT NULL AND industry <> '')
		AND company = company_name
        LIMIT 1;
    
    RETURN industry_name; 
END $$
DELIMITER ;