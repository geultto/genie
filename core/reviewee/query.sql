select
  channel_id, reviewers.user_ids as reviewers, reviewees.user_ids as reviewees
from (
  select
    channel_id, array_agg(distinct user_id order by user_id) as user_ids
  from
    geultto_4th_staging.user
  group by
    channel_id
) reviewers left join (
  select
    channel_id, array_agg(distinct user_id order by user_id) as user_ids
  from
    geultto_4th_staging.message_raw_{suffix}
  where
    (select countif(reaction.name = 'submit') from unnest(geultto_udf.parse_reactions(replace(reactions, '\'', '\"'))) as reaction) > 0
    and date(timestamp_micros(cast(cast(ts as numeric) * 1000000 as int64)), 'Asia/Seoul') between '2020-03-02' and '2020-03-15'
  group by
    channel_id
) reviewees using (channel_id)
order by
  channel_id
