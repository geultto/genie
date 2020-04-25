select
  -- 해당 due_ts 에 대한 review_mapping 이 존재하지 않고,
  -- 해당 due_ts 에 대한 review_mapping 을 정의하기에 충분한 message 가 있는 지 확인합니다.
  review_mapping_not_exists and message_exists as need_insert
from (
  select
    count(1) = 0 as review_mapping_not_exists
  from
    geultto_4th_prod.review_mapping
  where
    due_ts = geultto_udf.find_due_ts(current_timestamp())
) cross join (
  select
    max(ts) > timestamp_sub(geultto_udf.find_due_ts(current_timestamp()), interval 14 day) as message_exists
  from
    geultto_4th_prod.message
)
