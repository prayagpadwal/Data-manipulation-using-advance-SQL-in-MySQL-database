SELECT  *
FROM layoffs;

-- Creating a (duplicate table) Stagging table.
CREATE TABLE layoffs_stagging
LIKE Layoffs; 

INSERT layoffs_stagging
SELECT *
FROM Layoffs;

SELECT *
FROM layoffs_stagging;
-- END

-- Looking for duplicate rows.
WITH duplicate_cte AS 
(
SELECT *, ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off, 'date', stage, country, funds_raised_millions) as row_num
FROM layoffs_stagging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;
-- End

SELECT *
FROM layoffs_stagging
WHERE company = 'Airlift';


CREATE TABLE `layoffs_stagging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT *
FROM layoffs_stagging2;

INSERT INTO layoffs_stagging2
SELECT *, ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off, 'date', stage, country, funds_raised_millions) as row_num
FROM layoffs_stagging;

DELETE
FROM layoffs_stagging2
WHERE row_num > 1;

SELECT *
FROM layoffs_stagging2;

-- Standardizing the Data
-- 1. Trim the company column and then update
SELECT company, TRIM(company)
FROM layoffs_stagging2;

UPDATE layoffs_stagging2
SET company = TRIM(company);
-- end

-- 2. Updating industry column
SELECT *
FROM layoffs_stagging2
WHERE industry LIKE 'Crypto%';

UPDATE layoffs_stagging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';
-- end

-- 3. Updating the country column
SELECT DISTINCT(country), COUNT(*) OVER(PARTITION BY country) AS cnt_unq
FROM layoffs_stagging2
ORDER BY 1;

SELECT *
FROM layoffs_stagging2
WHERE country = 'United States.';

SELECT DISTINCT(country), TRIM(TRAILING '.' FROM country)
FROM layoffs_stagging2
ORDER BY 1;

UPDATE layoffs_stagging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';
-- end

-- 4. Update Date column so that we can perform time-series analysis later
SELECT date
FROM layoffs_stagging2;

SELECT date, STR_TO_DATE(date, '%m/%d/%y') as new_date
FROM layoffs_stagging2;

UPDATE layoffs_stagging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

SELECT *
FROM layoffs_stagging2;
-- end

-- 5. Looking for NULl values in the total_laid_off
SELECT *, COUNT(*) OVER(PARTITION BY total_laid_off)
FROM layoffs_stagging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

WITH CTE1 AS (
SELECT total_laid_off
FROM layoffs_stagging2
WHERE total_laid_off IS NULL)
SELECT COUNT(*)
FROM CTE1;
-- not helpful

-- 6. Filling the Null values
SELECT t1.industry, t2.industry
FROM layoffs_stagging2 t1
JOIN layoffs_stagging2 t2
	ON t1.company = t2.company
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;

UPDATE layoffs_stagging2 t1
JOIN layoffs_stagging2 t2
	ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;

UPDATE layoffs_stagging2
SET industry = NULL
WHERE industry = '';

SELECT *
FROM layoffs_stagging2
WHERE company LIKE 'Air%';

SELECT *
FROM layoffs_stagging2
WHERE industry IS NULL
OR industry = '';

SELECT *
FROM layoffs_stagging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;


ALTER TABLE layoffs_stagging2
DROP COLUMN row_num;

DELETE
FROM layoffs_stagging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT *
FROM layoffs_stagging2;

-- Final table

-- now i will try to find all null values
SELECT total_laid_off, percentage_laid_off, funds_raised_millions,
	COUNT(CASE WHEN total_laid_off IS NULL THEN 1 ELSE NULL END) OVER() AS cnt_NULL_total_laid,
    COUNT(CASE WHEN percentage_laid_off IS NULL THEN 1 ELSE NULL END) OVER() AS cnt_NULL_percentage_laid,
    COUNT(CASE WHEN funds_raised_millions IS NULL THEN 1 ELSE NULL END) OVER() AS cnt_NULL_funds_raised
FROM layoffs_stagging2
WHERE total_laid_off IS NULL
OR percentage_laid_off IS NULL
OR funds_raised_millions IS NULL;

-- verify
WITH count_total_laid AS 
	(SELECT total_laid_off
	FROM layoffs_stagging2
	WHERE total_laid_off IS NULL)
SELECT COUNT(*)
FROM count_total_laid



























