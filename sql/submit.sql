select
  due_ts, user_id,
  -- 글을 여러개 제출했을 수 있습니다. 1개라도 제출했으면 됩니다.
  logical_or(ts < due_ts) as is_valid
from
  geultto_6th_prod.message
where
  (select countif(r.name = 'submit' and user_id in unnest(r.user_ids)) from unnest(reactions) as r) > 0
group by
  due_ts, user_id
order by
  due_ts, user_id
