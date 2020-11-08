select
  logical_and(b) as b
from (
  select
    due_ts,
    case
      when date(due_ts, 'Asia/Seoul') <= '2020-11-15' then count(1) = 98 and count(user_id) = 98 and count(distinct(user_id)) = 98
      -- 2020-07-06 00:00:00+09:00 마감 글 부터, 사실상 활동하지 않는 UTGP424P9 user 를 배제합니다.
      else count(1) = 98 and count(user_id) = 98 and count(distinct(user_id)) = 98
    end as b
  from
    geultto_5th_staging.review_mapping
  group by
    due_ts
)
