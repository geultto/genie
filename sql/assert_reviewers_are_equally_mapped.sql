select
  -- reviewee 별 reviewers 가 최대한 분산되었는지 확인합니다.
  -- e.g. 1,2,3,4 가 아니라 2,2,3,3 으로 분산되었는지 확인.
  logical_and(cnt_distinct <= 2) as b
from (
  select
    due_ts, channel_id, count(distinct(cnt)) as cnt_distinct
  from (
    select
      due_ts, channel_id, reviewee_id, count(1) as cnt
    from
      geultto_6th_prod.review_mapping rm
      cross join unnest(reviewee_ids) as reviewee_id
      join geultto_6th_prod.user u using (user_id)
    group by
      due_ts, channel_id, reviewee_id
  )
  group by
    due_ts, channel_id
)
