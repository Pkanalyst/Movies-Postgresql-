create table moviesdata(
MovieID varchar,
Title varchar,
Genre varchar,
ReleaseYear varchar,
ReleaseDate date,
Country varchar,
BudgetUSD decimal,
US_BoxOfficeUSD decimal,
Global_BoxOfficeUSD decimal,
Opening_Day_SalesUSD decimal,
One_Week_SalesUSD decimal,
IMDbRating decimal,
RottenTomatoesScore int,
NumVotesIMDb int,
NumVotesRT int,
Director varchar,
LeadActor varchar);


copy moviesdata
FROM 'E:\Datasets\Movie Dataset for Analytics & Visualization\movies_dataset.csv' WITH (FORMAT CSV, HEADER TRUE, DELIMITER ',');


-- ##################################################################################
-- ########################### Movie dataset sql project ############################
-- ##################################################################################

-- #################################################
-- ## section 1: basic queries (filtering and limits)
-- #################################################

-- 1. basic query: top-rated movies
-- question: find the top 10 movies based on their imdbrating. 
--           include the title and releaseyear.

select title,releaseyear,imdbrating
from moviesdata
order by imdbrating desc,numvotesimdb desc -- using numvotesimdb as a tie-breaker for rating
limit 10;

-- 2. basic query: recent releases
-- question: list all movie titles and their director that were released in a specific year (e.g., 2024).

select title,director
from moviesdata
where releaseyear = '2024'; -- replace 2024 with the year you wish to query

-- 3. basic query: specific genre count
-- question: how many movies in the dataset belong to the 'action' genre?

select count(movieid) as action_movie_count
from moviesdata
where genre like '%action%';

-- 4. basic query: highest budget
-- question: find the title and director of the movie with the highest budgetusd.

select title,director
from moviesdata
order by budgetusd desc
limit 1;


---
-- ######################################################
-- ## section 2: intermediate queries (grouping and sums)
-- ######################################################

-- 5. intermediate query: average rating by director
-- question: calculate the average imdbrating for each director. 
--           only include directors who have directed more than 3 movies.

select director,count(movieid) as total_movies,avg(imdbrating) as avg_imdb_rating
from moviesdata
group by director
having count(movieid) > 3
order by avg_imdb_rating desc;

-- 6. intermediate query: genre performance (us box office revenue)
-- question: determine the total us_boxofficeusd revenue for each genre. which genre has the highest cumulative box office?

select genre,sum(us_boxofficeusd) as total_us_revenue
from moviesdata
group by genre
order by total_us_revenue desc;

-- 7. intermediate query: yearly release trend
-- question: count the total number of movies released in each releaseyear.

select releaseyear,count(movieid) as total_movies_released
from moviesdata
group by releaseyear
order by releaseyear desc;

-- 8. intermediate query: profitability
-- question: identify the title and director of the movie with the highest "profit" (calculated as global_boxofficeusd minus budgetusd).

select title,director,(global_boxofficeusd - budgetusd) as net_profit
from moviesdata
where budgetusd is not null and global_boxofficeusd is not null
order by net_profit desc
limit 1;

-- 9. intermediate query: lead actor performance & consistency
-- question: identify the leadactor who has the highest average global box office revenue. 
--           only include actors who have starred in at least 5 movies.

select leadactor,count(movieid) as total_movies,avg(global_boxofficeusd) as avg_global_revenue
from moviesdata
where global_boxofficeusd is not null
group by leadactor
having count(movieid) >= 5
order by avg_global_revenue desc;

-- 10. intermediate query: director-actor collaboration analysis
-- question: identify the director and leadactor pair that has collaborated on the most movies.

select director,leadactor,count(movieid) as collaboration_count
from moviesdata
where director is not null and leadactor is not null
group by director,leadactor
order by collaboration_count desc
limit 5;

-- 11. intermediate query: release day analysis (date functions)
-- question: determine which day of the week (e.g., monday, friday) results in the highest average opening_day_salesusd.

select extract(dow from releasedate) as day_of_week_num, -- dow=day of week (0=sunday, 6=saturday)
    avg(opening_day_salesusd) as avg_opening_day_sales
from moviesdata
where releasedate is not null and opening_day_salesusd is not null
group by day_of_week_num
order by avg_opening_day_sales desc;


---
-- #########################################################
-- ## section 3: advanced queries (window functions, subqueries, case)
-- #########################################################

-- 12. advanced query: director's best film (window function)
-- question: for each director, find the title of their film that has the highest imdbrating.

with rankedmovies as (
    select title,director,imdbrating,
        row_number() over(partition by director order by imdbrating desc, numvotesimdb desc) as rn
    from moviesdata
)
select title,director,imdbrating
from rankedmovies
where rn = 1
order by imdbrating desc;

-- 13. advanced query: high-budget, low-rating (subqueries)
-- question: identify movies where the budgetusd is above the dataset's overall average budget, 
--           but the imdbrating is below the overall average rating.

select title,budgetusd,imdbrating
from moviesdata
where budgetusd > (select avg(budgetusd) from moviesdata)
    and imdbrating < (select avg(imdbrating) from moviesdata)
order by budgetusd desc;

-- 14. advanced query: success rate by country (conditional logic)
-- question: calculate the percentage of movies from each country that have a rottentomatoesscore of $80\%$ or higher.

select country,
    cast(sum(case when rottentomatoesscore >= 80 then 1 else 0 end) as real) * 100 / count(movieid) as percent_success
from moviesdata
where country is not null
group by country
order by percent_success desc;

-- 15. advanced query: comparison of box office (calculated percentage)
-- question: compare the opening_day_salesusd to the one_week_salesusd for each film. calculate the percentage drop-off or increase.

select title,opening_day_salesusd,one_week_salesusd,
    ((one_week_salesusd - opening_day_salesusd) * 100.0 / opening_day_salesusd) as percentage_change
from moviesdata
where opening_day_salesusd is not null and opening_day_salesusd > 0 and one_week_salesusd is not null
order by percentage_change desc;

-- 16. advanced query: box office drop-off rate by genre (focus on decay)
-- question: calculate the average box office drop-off/increase percentage from the opening_day_salesusd 
--           to the one_week_salesusd for each genre.

select genre,
    avg((one_week_salesusd - opening_day_salesusd) * 100.0 / opening_day_salesusd) as avg_weekly_change_percent
from moviesdata
where opening_day_salesusd is not null and opening_day_salesusd > 0 and one_week_salesusd is not null
group by genre
order by avg_weekly_change_percent desc;

-- 17. advanced query: highly rated, low-budget successes (percentile comparison)
-- question: find movies that have an imdbrating in the top $10\%$ of all ratings, but whose budgetusd is in the bottom $25\%$ of all budgets.

select title,imdbrating,budgetusd
from moviesdata
where imdbrating >= (select percentile_cont(0.9) within group (order by imdbrating) from moviesdata) 
    and budgetusd <= (select percentile_cont(0.25) within group (order by budgetusd) from moviesdata)
order by imdbrating desc,budgetusd asc;