select
  message_raw.channel_id,
  message_distinct.ts,
  user_id,
  array(select r from unnest(geultto_udf.parse_reactions(replace(reactions, '\'', '\"'))) as r where r.name in ('submit', 'feedback', 'pass')) as reactions,
  timestamp_micros(cast(cast(thread_ts as numeric) * 1000000 as int64)) as thread_ts,
  parent_user_id,
  text,
  client_msg_id,
  insert_ts,
  due_ts
from
  geultto_4th_prod.message_raw
  join (
    select
      channel_id, ts, insert_ts, due_ts
    from (
      select
        channel_id, ts, insert_ts, due_ts,
        row_number() over (partition by channel_id, ts order by sorting_ts) as rn
      from (
        select
          channel_id, ts, insert_ts, due_ts,
          if(insert_ts >= due_ts, insert_ts, timestamp_add(current_timestamp(), interval timestamp_diff(due_ts, insert_ts, second) second)) as sorting_ts
        from (
          select
            channel_id,
            timestamp_micros(cast(cast(ts as numeric) * 1000000 as int64)) as ts,
            timestamp_micros(time_ms) as insert_ts,
            geultto_udf.find_due_ts(timestamp_micros(cast(cast(ts as numeric) * 1000000 as int64))) as due_ts
          from
            geultto_4th_prod.message_raw
        )
      )
    )
    where
      rn = 1
  ) message_distinct on message_raw.channel_id = message_distinct.channel_id
    and timestamp_micros(cast(cast(message_raw.ts as numeric) * 1000000 as int64)) = message_distinct.ts
    and time_ms = unix_micros(insert_ts)
where
  (select count(1) from unnest(geultto_udf.parse_reactions(replace(reactions, '\'', '\"'))) as r where r.name in ('submit', 'feedback', 'pass')) > 0
order by
  ts
