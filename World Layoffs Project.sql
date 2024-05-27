-- ------------------------------------DATA CLEANING ---------------------

SELECT *
FROM layoffs_working_data;

CREATE TABLE layoffs_working_data
LIKE layoffs;

INSERT layoffs_working_data
SELECT *
FROM layoffs;

SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS Row_Num
FROM layoffs_working_data;

WITH Duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS Row_Num
FROM layoffs_working_data
)
SELECT *
FROM  Duplicate_cte
WHERE Row_Num > 1;

-- --------------------CREATING A DIPLICATE TABLE TO REMOVE ALL DUPLICATES FROM THE DATASET------------------------

CREATE TABLE `layoffs_working_data2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT *
FROM layoffs_working_data2;

INSERT INTO layoffs_working_data2
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS Row_Num
FROM layoffs_working_data;

DELETE
FROM layoffs_working_data2
WHERE row_num > 1;

-- -------------------STANDARDIZING THE DATA------------------------------------

-- REMOVING WHITE SPACES

SELECT company, TRIM(company)
FROM layoffs_working_data2;

UPDATE layoffs_working_data2
SET company = TRIM(company);

SELECT *
FROM layoffs_working_data2;

SELECT DISTINCT(industry)
FROM layoffs_working_data2
ORDER BY 1;

SELECT *
FROM layoffs_working_data2
WHERE industry LIKE 'crypto%';

UPDATE layoffs_working_data2
SET industry = 'crypto'
WHERE industry LIKE 'crypto%';

SELECT DISTINCT(country), TRIM(TRAILING '.' FROM country) 
FROM layoffs_working_data2
ORDER BY 1;

UPDATE layoffs_working_data2
SET country = TRIM(TRAILING '.' FROM country) 
WHERE country LIKE 'United States%';

SELECT DISTINCT(country)
FROM layoffs_working_data2;

-- --FORMATING THE DATE COLUMN

SELECT `date`,
STR_TO_DATE(`date`, '%m/%d/%Y')
FROM layoffs_working_data2;

UPDATE layoffs_working_data2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

ALTER TABLE layoffs_working_data2
MODIFY COLUMN `date` DATE;

-- -------------- REMOVING/POPULATING NULL AND BLANK VALUES --------------------------------------
SELECT *
FROM layoffs_working_data2
WHERE industry IS NULL;

UPDATE layoffs_working_data2
SET industry = NULL
WHERE industry = '';

SELECT *
FROM layoffs_working_data2 t1
	JOIN layoffs_working_data2 t2
	ON t1.company = t2.company
WHERE (t1.industry IS NULL OR t2.industry = '')
AND t2.industry IS NOT NULL;

UPDATE layoffs_working_data2 t1
JOIN layoffs_working_data2 t2
	ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;


SELECT *
FROM layoffs_working_data2
WHERE company = 'Airbnb';

-- ----------------------DELETING COLUMNS NOT USEFUL TO OUR ANALYSIS ------------------------------------------

-- NOTE: The original dataset is intact, all operations where performed on the dataset we duplicated.

SELECT *
FROM layoffs_working_data2
WHERE total_laid_off IS NULL 
AND percentage_laid_off IS NULL;

SELECT *
FROM layoffs_working_data2;

ALTER TABLE layoffs_working_data2
DROP COLUMN row_num;


-- ------------------------------------------- EXPLORATORY DATA ANALYSIS ---------------------------------------------------

SELECT MIN(`date`), MAX(`date`)
FROM layoffs_working_data2;

SELECT MAX(total_laid_off)
FROM layoffs_working_data2;

SELECT MAX(percentage_laid_off)
FROM layoffs_working_data2;

SELECT *
FROM layoffs_working_data2
WHERE percentage_laid_off = 1
ORDER BY total_laid_off DESC;

SELECT *
FROM layoffs_working_data2
WHERE percentage_laid_off = 1
ORDER BY funds_raised_millions DESC;

SELECT industry, SUM(total_laid_off)
FROM layoffs_working_data2
GROUP BY industry
ORDER BY SUM(total_laid_off) DESC;

SELECT country, SUM(total_laid_off)
FROM layoffs_working_data2
GROUP BY country
ORDER BY SUM(total_laid_off) DESC;

SELECT YEAR(`date`), SUM(total_laid_off)
FROM layoffs_working_data2
GROUP BY YEAR(`date`)
ORDER BY SUM(total_laid_off) DESC;

SELECT Stage, SUM(total_laid_off)
FROM layoffs_working_data2
GROUP BY Stage
ORDER BY SUM(total_laid_off) DESC;

SELECT SUBSTRING(`date`, 1, 7) AS `MONTH`, SUM(total_laid_off)
FROM layoffs_working_data2
WHERE SUBSTRING(`date`, 1, 7) IS NOT NULL
GROUP BY `MONTH` 
ORDER BY 1 ASC;

WITH Rolling_total AS
(
SELECT SUBSTRING(`date`, 1, 7) AS `MONTH`, SUM(total_laid_off) AS total_laid_off
FROM layoffs_working_data2
WHERE SUBSTRING(`date`, 1, 7) IS NOT NULL
GROUP BY `MONTH` 
ORDER BY 1 ASC
)
SELECT `MONTH`, total_laid_off,
SUM(total_laid_off) OVER (ORDER BY `MONTH`) AS rolling_total
FROM Rolling_total;

SELECT company, SUM(total_laid_off)
FROM layoffs_working_data2
GROUP BY company
ORDER BY SUM(total_laid_off) DESC;

SELECT company, YEAR(`date`), SUM(total_laid_off)
FROM layoffs_working_data2
GROUP BY company, YEAR(`date`)
ORDER BY  SUM(total_laid_off) DESC;

WITH Company_Year(Company, Years, Total_laid_off) AS
(
SELECT company, YEAR(`date`), SUM(total_laid_off)
FROM layoffs_working_data2
GROUP BY company, YEAR(`date`)
ORDER BY  SUM(total_laid_off) DESC
), Company_Year_Rank AS
(
SELECT *,
DENSE_RANK() OVER (PARTITION BY years ORDER BY Total_laid_off DESC) AS Ranking
FROM Company_Year
WHERE Years IS NOT NULL
)
SELECT *
FROM  Company_Year_Rank
WHERE Ranking <= 10 AND Years = 2020;

SELECT industry, YEAR(`date`), SUM(total_laid_off)
FROM layoffs_working_data2
GROUP BY industry, YEAR(`date`)
ORDER BY  SUM(total_laid_off) DESC;

WITH Industry_Year(Industry, Years, Total_laid_off) AS
(
SELECT industry, YEAR(`date`), SUM(total_laid_off)
FROM layoffs_working_data2
GROUP BY industry, YEAR(`date`)
ORDER BY  SUM(total_laid_off) DESC
), Industry_Year_Rank AS
(
SELECT *,
DENSE_RANK() OVER (PARTITION BY years ORDER BY Total_laid_off DESC) AS Ranking
FROM Industry_Year
WHERE Years IS NOT NULL
)
SELECT *
FROM  Industry_Year_Rank
WHERE Ranking <= 5 AND Years = 2022;

SELECT location, YEAR(`date`), SUM(total_laid_off)
FROM layoffs_working_data2
GROUP BY location, YEAR(`date`)
ORDER BY  SUM(total_laid_off) DESC;

WITH Location_Year(Industry, Years, Total_laid_off) AS
(
SELECT location, YEAR(`date`), SUM(total_laid_off)
FROM layoffs_working_data2
GROUP BY location, YEAR(`date`)
ORDER BY  SUM(total_laid_off) DESC
), Location_Year_Rank AS
(
SELECT *,
DENSE_RANK() OVER (PARTITION BY years ORDER BY Total_laid_off DESC) AS Ranking
FROM Location_Year
WHERE Years IS NOT NULL
)
SELECT *
FROM  Location_Year_Rank
WHERE Ranking <= 5;

SELECT *
FROM layoffs_working_data2

