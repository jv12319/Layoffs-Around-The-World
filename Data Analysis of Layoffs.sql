select *
from layoffs_staging2;

select max(total_laid_off), max(percentage_laid_off)
from layoffs_staging2;

select *
from layoffs_staging2
where percentage_laid_off = 1
order by funds_raised_millions desc;

select company, sum(total_laid_off) as sum
from layoffs_staging2
group by company
order by sum desc;

select min(date), max(date)
from layoffs_staging2;

select industry, sum(total_laid_off) as sum
from layoffs_staging2
group by industry
order by sum desc;

select country, sum(total_laid_off) as sum
from layoffs_staging2
group by country
order by sum desc;

select YEAR(date), sum(total_laid_off) as sum
from layoffs_staging2
group by 1
order by 1 desc;

select substring(date, 1,7) as Month, sum(total_laid_off) as sum
from layoffs_staging2
where substring(date, 1,7) is not null
group by 1
order by 1;

with Rolling_total as
(
select substring(date, 1,7) as Month, sum(total_laid_off) as total
from layoffs_staging2
where substring(date, 1,7) is not null
group by 1
order by 1
)
Select Month, total, sum(total) over(order by month) as rolling_totals
from Rolling_total;

select company, year(date), sum(total_laid_off)
from layoffs_staging2
group by 1, 2
order by 1 asc;

with company_year as 
(
select company, year(date) as years, sum(total_laid_off) as total_laid_off
from layoffs_staging2
group by 1, 2
) , company_ranking_by_year as (
select *, dense_rank() over (partition by years order by total_laid_off desc) as Ranking
from company_year
where years is not null
)
select *
from company_ranking_by_year
where ranking <= 3;

-- More analysis of the data
-- 1.Basic data exploration

-- Total layoffs in dataset
select sum(total_laid_off) as total_laid_off
from layoffs_staging2;

-- number of Unique companies in dataset
select count(distinct company) as num_of_companies_in_dataset
from layoffs_staging2;

-- Industry with the most layoffs
select distinct industry, sum(total_laid_off) as total_laid_off
from layoffs_staging2
group by industry
order by total_laid_off desc
limit 1;

-- 2. Aggregation and Summarization

-- Average percentage of layoffs across all companies
select  avg(cast(percentage_laid_off AS decimal))
from layoffs_staging2
where percentage_laid_off is not null;

-- Total funds raised by companies that reported layoffs in each country
select country, sum(funds_raised_millions) as total_funds_raised_by_country
from layoffs_staging2
group by country;

-- Total layoffs by industry
select industry, sum(total_laid_off)
from layoffs_staging2
group by industry;

-- 3. Filtering and Conditions

-- 	All companies that have laid off more than 100 employees
select company, sum(total_laid_off)
from layoffs_staging2
where total_laid_off > 100
group by company
order by sum(total_laid_off) asc;

-- Companies in US that have laid off atleast 50%
select company, cast(percentage_laid_off AS decimal(10,2)) as new_percentage_laid_off
from layoffs_staging2
where country = "United States" 
and percentage_laid_off > 0.5
group by company, new_percentage_laid_off
order by new_percentage_laid_off asc;

-- Comapnies who raised more than 100 million but still laid off more than 20%
select company, sum(funds_raised_millions), cast(percentage_laid_off AS decimal(10,2)) as new_percentage_laid_off, date
from layoffs_staging2
where funds_raised_millions > 100
and percentage_laid_off > 0.2
group by company, percentage_laid_off, date
order by company asc;

-- 4. Grouping and Sorting

-- Industries with the highest average percentage of layoffs
select industry, avg(cast(percentage_laid_off AS decimal)) as new_percentage_laid_off
from layoffs_staging2
where percentage_laid_off is not null
group by industry
order by new_percentage_laid_off desc;

-- The top 5 companies based on the total number of employees laid off
select company, sum(total_laid_off) as total_laid_off
from layoffs_staging2
group by company
order by total_laid_off desc
limit 5;

-- Average funds raised by country
select country, avg(funds_raised_millions) as avg_funds_raised
from layoffs_staging2
group by country;

-- 5. Time Based Analysis

-- Month with the highest total layoffs
select month(date) as month, sum(total_laid_off) as total_laid_off
from layoffs_staging2
group by month
order by total_laid_off desc;

-- Trend of layoffs over time by calculating the total layoffs per year
select year(date) as year, sum(total_laid_off) as total_laid_off_by_year
from layoffs_staging2 
group by year
order by year;

-- How the stage of companies (e.g., early, growth, late-stage) correlates with layoffs over different dates
select stage, year(date) as 'year', sum(total_laid_off) as total_laid_off
from layoffs_staging2
where stage is not null
and date is not null
and total_laid_off is not null
group by stage, year
order by stage;

-- 6. Subqueries

-- Companies that have raised more than the average funds raised across all companies
select company, sum(funds_raised_millions)
from layoffs_staging2
where funds_raised_millions > 
    (select avg(funds_raised_millions)
    from layoffs_staging2)
group by company
order by company;

select avg(funds_raised_millions)
    from layoffs_staging2;

-- Companies that have laid off more employees than the overall average number of layoffs across all companies
select company, sum(total_laid_off)
from layoffs_staging2
where total_laid_off > 
	(select avg(total_laid_off)
	from layoffs_staging2)
group by company;

select avg(total_laid_off)
	from layoffs_staging2;
    
-- 7. Advanced Analysis

-- The relationship between the funds raised and the percentage of layoffs
select company, sum(funds_raised_millions) as sum_raised, sum(cast(percentage_laid_off as decimal(10,2))) as sum_percentage_laid_off
from layoffs_staging2
where funds_raised_millions is not null
and percentage_laid_off is not null
group by company, percentage_laid_off
order by percentage_laid_off asc;

-- Industries where late-stage companies are laying off employees at a higher rate than early-stage companies
select company, stage, avg(total_laid_off)
from layoffs_staging2
where stage =  "Post-IPO"
or stage = "Seed"
group by company, stage
order by stage desc;

-- How layoffs vary by location within the same country
select location, country, sum(total_laid_off)
from layoffs_staging2
group by country, location
order by country;

 










