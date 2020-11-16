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
  geultto_5th_prod.message_raw
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
          -- due_ts 이후 메시지 중 가장 앞의 것을 취하고, due_ts 이후 메시지가 없다면 due_ts 이전 메시지 중 가장 뒤의 것을 취합니다.
          if(insert_ts >= due_ts, insert_ts, timestamp_add(current_timestamp(), interval timestamp_diff(due_ts, insert_ts, second) second)) as sorting_ts
        from (
          select
            channel_id, ts, insert_ts,
            -- feedback 은 thread_ts 에 대한 마감 시각 + 2주로 마감 시각을 결정합니다. feedback thread 달고 -> 리뷰어 지정되고 -> feedback emoji 만 다는 케이스를 고려하기 위함입니다.
            -- 이때 message_raw 에 처음 insert 될때 feedback emoji 가 없었다가 이후 달리는 케이스를 커버하기 위해 thread_ts is not null 의 널널한 조건을 사용합니다.
            -- submit, feedback emoji 를 실수로 함께 매단 케이스가 1건 존재하여 case when 절에서 우선 submit or pass 여부로 분기합니다.
            case
              when user_id = 'UT3DE17S7' and ts = '2020-02-19 12:51:14.020 UTC' then '2020-03-15 15:00:00 UTC' -- 훈련소 입소로 2주차 글 미리 제출한 것 따로 처리.
              when (select countif(r.name in ('submit', 'pass')) from unnest(reactions) as r) > 0 then geultto_udf.find_due_ts(ts)
              when thread_ts is not null then timestamp_add(geultto_udf.find_due_ts(thread_ts), interval 14 day)
              else geultto_udf.find_due_ts(ts)
            end as due_ts
          from (
            select
              channel_id, user_id,
              timestamp_micros(cast(cast(ts as numeric) * 1000000 as int64)) as ts,
              timestamp_micros(cast(cast(thread_ts as numeric) * 1000000 as int64)) as thread_ts,
              timestamp_micros(time_ms) as insert_ts,
              array(select r from unnest(geultto_udf.parse_reactions(replace(reactions, '\'', '\"'))) as r where r.name in ('submit', 'feedback', 'pass')) as reactions
            from
              geultto_5th_prod.message_raw
          )
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
