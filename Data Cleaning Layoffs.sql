Select *
from layoffs;

-- 1. Removing Duplicates
-- 2. Standardize all of the data
-- 3. Fix null/blank values
-- 4. Remove the cols/rows that are not needed

CREATE TABLE layoffs_staging
Like layoffs;

Select *
from layoffs_staging;

INSERT layoffs_staging
SELECT *
FROM layoffs;

-- Lines 9-17 we created a table with the original data to make the future desired changes
-- without changing the raw data.


WITH duplicates_cte AS 
(
Select *,
ROW_NUMBER() 
over(partition by company, location, industry, total_laid_off,
 percentage_laid_off, date, stage, country, funds_raised_millions) as row_num
from layoffs_staging
)
Select * 
from duplicates_cte
where row_num > 1;

Select *
from layoffs_staging
where company = 'casper';

CREATE TABLE `layoffs_staging2` (
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

INSERT INTO layoffs_staging2
Select *,
ROW_NUMBER() 
over(partition by company, location, industry, total_laid_off,
 percentage_laid_off, date, stage, country, funds_raised_millions) as row_num
from layoffs_staging;


select * 
from layoffs_staging2;

DELETE 
from layoffs_staging2
WHERE row_num > 1;

-- Lines 23-33 we identified duplicates in the data by using a cte 
-- to partition through the data and assign a unique number to duplicate rows

-- Line 39-65 we created a new table to insert the new partitioned data
-- made by the cte into layoff_staging2. This was done because I can't delete the cte on its
-- own. After inserting the newly organized data into layoffs_staging2 we deleted any rows 
-- that had a row_num value that was more than 1 meaning it was a duplicate.

-- PT2 Standardizing data

SELECT distinct company,  TRIM(company)
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company = TRIM(company);

select distinct industry
from layoffs_staging2
Order by 1;

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

select distinct country
from layoffs_staging2
Order by 1;

UPDATE layoffs_staging2
SET country = 'United States'
WHERE country LIKE 'United States%';


SELECT `date`, str_to_date(`date`, '%m/%d/%Y') as new_date
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET `date` = str_to_date(`date`, '%m/%d/%Y');

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL;
-- PT3

select distinct industry
from layoffs_staging2
Order by 1;

SELECT *
FROM layoffs_staging2
WHERE industry IS NULL
OR industry = '';

SELECT *
FROM layoffs_staging2
WHERE company = 'Airbnb';

SELECT st1.company, st2.company, st1.industry, st2.industry
FROM layoffs_staging2 st1
JOIN layoffs_staging2 st2
	ON st1.company = st2.company
    AND st1.location = st2.location
WHERE (st1.industry IS NULL OR st1.industry = '')
AND st2.industry IS NOT NULL;


SELECT *
FROM layoffs_staging2
WHERE company LIKE 'Ball%';

UPDATE layoffs_staging2
SET industry = null
WHERE industry = '';

UPDATE layoffs_staging2 st1
JOIN layoffs_staging2 st2
	ON st1.company = st2.company
SET st1.industry = st2.industry
WHERE (st1.industry IS NULL OR st1.industry = '')
AND st2.industry IS NOT NULL;

-- PT4

SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

DELETE 
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

ALTER TABLE layoffs_staging2
DROP column row_num;

SELECT *
FROM layoffs_staging2;
