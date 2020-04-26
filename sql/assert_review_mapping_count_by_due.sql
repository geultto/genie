select
  logical_and(b) as b
from (
  select
    due_ts, count(1) = 68 and count(user_id) = 68 and count(distinct(user_id)) = 68 as b
  from
    geultto_4th_prod.review_mapping
  group by
    due_ts
)
