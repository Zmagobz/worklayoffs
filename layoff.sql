-- Data Cleaning

SELECT * 
FROM world_layoffs.layoffs;

-- Outline for data cleaning:
-- 1. check for duplicates and remove any them
-- 2. Standardize the  data
-- 3. Look at null values or blank values
-- 4. Remove any columns 

-- Create a staging table. This is the one we will work in and clean the data to work on so that we can be able to keep the original
CREATE TABLE world_layoffs.layoffs_staging 
LIKE world_layoffs.layoffs;

-- We now insert the data from layoffs to layoff_staginh
INSERT world_layoffs.layoffs_staging 
SELECT * 
FROM world_layoffs.layoffs;

-- 1. Remove Duplicates

# First let's check for duplicates

SELECT *
FROM world_layoffs.layoffs_staging;

SELECT *,
ROW_NUMBER() OVER (
PARTITION BY company, industry, total_laid_off,`date`) AS row_num
FROM world_layoffs.layoffs_staging;

WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER (
PARTITION BY company, location, 
industry, total_laid_off, percentage_laid_off, `date`, stage
, country, funds_raised_millions) AS row_num
FROM world_layoffs.layoffs_staging
)
SELECT * 
FROM duplicate_cte
WHERE row_num > 1;


-- Once you've reviewed, delete the duplicate rows from the staging table.
WITH duplicate_cte AS (
  SELECT *,
    ROW_NUMBER() OVER (
      PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions
      ORDER BY `date`
    ) AS row_num
  FROM world_layoffs.layoffs_staging
)
DELETE FROM world_layoffs.layoffs_staging
WHERE (company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) IN (
  SELECT company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions
  FROM duplicate_cte
  WHERE row_num > 1
);


-- one solution, which I think is a good one. Is to create a new column and add those row numbers in. Then delete where row numbers are over 2, then delete that column
-- so let's do it!!

-- Add a row_num column to the staging table
ALTER TABLE world_layoffs.layoffs_staging ADD row_num INT;

-- View all records in the staging table
SELECT * FROM world_layoffs.layoffs_staging;

-- Create a new staging2 table to store data with row numbers for duplicate detection
CREATE TABLE `world_layoffs`.`layoffs_staging2` (
  `company` TEXT,
  `location` TEXT,
  `industry` TEXT,
  `total_laid_off` INT DEFAULT NULL,
  `percentage_laid_off` TEXT,
  `date` TEXT,
  `stage` TEXT,
  `country` TEXT,
  `funds_raised_millions` INT DEFAULT NULL,
  `row_num` INT
);

-- Insert data into the layoffs_staging2 table, assigning row numbers to detect duplicates
INSERT INTO `world_layoffs`.`layoffs_staging2`
(`company`,
`location`,
`industry`,
`total_laid_off`,
`percentage_laid_off`,
`date`,
`stage`,
`country`,
`funds_raised_millions`,
`row_num`)
SELECT 
  `company`,
  `location`,
  `industry`,
  `total_laid_off`,
  `percentage_laid_off`,
  `date`,
  `stage`,
  `country`,
  `funds_raised_millions`,
  ROW_NUMBER() OVER (
    PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions
    ORDER BY `date`
  ) AS row_num
FROM world_layoffs.layoffs_staging;

-- Delete rows from layoffs_staging2 where row_num is 2 or greater (removing duplicates)
DELETE FROM world_layoffs.layoffs_staging2
WHERE row_num >= 2;

-- Optional: Select remaining records after removing duplicates
SELECT * FROM world_layoffs.layoffs_staging2;

-- 2. Standardize Data

SELECT * 
FROM world_layoffs.layoffs_staging2;

-- if we look at industry it looks like we have some null and empty rows, let's take a look at these
SELECT DISTINCT industry
FROM world_layoffs.layoffs_staging2
ORDER BY industry;

SELECT *
FROM world_layoffs.layoffs_staging2
WHERE industry LIKE 'Crypto%';

UPDATE world_layoffs.layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

-- everything looks good except apparently we have some "United States" and some "United States." with a period at the end. Let's standardize this.
SELECT DISTINCT country
FROM world_layoffs.layoffs_staging2
ORDER BY country;

UPDATE world_layoffs.layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country);

-- Fixing the date for when we need it for data visualization
SELECT * 
FROM world_layoffs.layoffs_staging2;

-- we can use str to date to update this field
UPDATE world_layoffs.layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

-- now we can convert the data type properly
ALTER TABLE world_layoffs.layoffs_staging2
MODIFY COLUMN `date` DATE;

SELECT *
FROM world_layoffs.layoffs_staging2
WHERE total_laid_off IS NULL 
AND percentage_laid_off IS NULL;

UPDATE world_layoffs.layoffs_staging2
SET industry = NULL
WHERE industry ='';

SELECT *
FROM world_layoffs.layoffs_staging2
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;




SELECT *
FROM world_layoffs.layoffs_staging2 t1
JOIN world_layoffs.layoffs_staging2 t2
	ON t1.company = t2.company
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;

UPDATE world_layoffs.layoffs_staging2 t1
JOIN world_layoffs.layoffs_staging2 t2
	ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

SELECT *
FROM world_layoffs.layoffs_staging2
WHERE total_laid_off IS NULL 
AND percentage_laid_off IS NULL;

-- Delete Useless data we can't really use
DELETE
FROM world_layoffs.layoffs_staging2
WHERE total_laid_off IS NULL 
AND percentage_laid_off IS NULL;

SELECT *
FROM world_layoffs.layoffs_staging2;

ALTER TABLE world_layoffs.layoffs_staging2
DROP COLUMN row_num;


