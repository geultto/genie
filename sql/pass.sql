select
  due_ts, user_id, ts, is_successive, ordinal, not is_successive and ordinal <= 2 as is_valid
from (
  select
    user_id, due_ts, ts, is_successive, countif(not is_successive) over (partition by user_id order by due_ts rows between unbounded preceding and current row) as ordinal
  from (
    select
      user_id, due_ts, ts, ifnull(timestamp_diff(due_ts, due_ts_prev, day) <= 14, false) as is_successive
    from (
      select
        user_id, due_ts, ts,
        lag(due_ts, 1) over (partition by user_id order by due_ts) as due_ts_prev,
      from
        geultto_6th_prod.message
      where
        (select countif(r.name = 'pass' and user_id in unnest(r.user_ids)) from unnest(reactions) as r) > 0
    )
  )
)
order by
  due_ts, ts
