select
  channel_id, reviewers.user_ids as reviewers, reviewees.user_ids as reviewees
from (
  select
    channel_id, array_agg(distinct user_id order by user_id) as user_ids
  from
    geultto_4th_prod.user
  group by
    channel_id
) reviewers left join (
  select
    channel_id, array_agg(distinct user_id order by user_id) as user_ids
  from
    geultto_4th_prod.message
  where
    (select countif(reaction.name = 'submit' and user_id in unnest(reaction.user_ids)) from unnest(reactions) as reaction) > 0
    and date(ts, 'Asia/Seoul') between date_sub('{date_kr_due}', interval 13 day) and '{date_kr_due}'
  group by
    channel_id
) reviewees using (channel_id)
order by
  channel_id
