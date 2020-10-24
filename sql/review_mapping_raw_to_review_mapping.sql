insert geultto_4th_prod.review_mapping
select
  user_id,
  geultto_udf.parse_reviewee_ids(replace(reviewee_ids, '\'', '\"')) as reviewee_ids,
  geultto_udf.find_due_ts(timestamp_micros(time_ms)) as due_ts,
  timestamp_micros(time_ms) as insert_ts,
from
  {table_review_mapping_raw}
