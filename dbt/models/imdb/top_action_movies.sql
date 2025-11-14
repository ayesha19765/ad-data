{{ config(materialized='table') }}

-- Make this robust to column name differences
with src as (
  select
    coalesce(movie_name, title) as title,
    safe_cast(rating as float64) as rating
  from {{ source('imdb','action') }}
)
select *
from src
where rating is not null
order by rating desc
limit 100
