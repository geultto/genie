select
  channel_id, reviewers.user_ids as reviewers, reviewees.user_ids as reviewees
from (
  select
    channel_id, array_agg(distinct user_id order by user_id) as user_ids
  from
    geultto_4th_prod.user
  where
    not user_id = 'UTGP424P9' -- 사실상 활동하지 않는 유저 배제.
  group by
    channel_id
) reviewers left join (
  select
    channel_id, array_agg(distinct user_id order by user_id) as user_ids
  from
    geultto_4th_prod.message
  where
    (select countif(reaction.name = 'submit' and user_id in unnest(reaction.user_ids)) from unnest(reactions) as reaction) > 0
    -- 마감일 직후에 geultto_udf.find_due_ts(current_timestamp()) 를 호출하면 다음 마감일이 됩니다.
    -- 직전 마감일 = 다음 마감일 14일 전을 due_ts 로 하는 submit 이 필요합니다.
    and due_ts = timestamp_sub(geultto_udf.find_due_ts(current_timestamp()), interval 14 day)
  group by
    channel_id
) reviewees using (channel_id)
