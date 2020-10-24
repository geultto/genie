select
  logical_and(
    -- reviewee_ids 는 딱 2명이어야 하고,
    array_length(reviewee_ids) = 2
    -- 그 2명은 서로 달라야 하며,
    and (select count(distinct(reviewee_id)) from unnest(reviewee_ids) as reviewee_id) = 2
    -- 그 2명에 자기 자신은 없어야 합니다.
    and user_id not in unnest(reviewee_ids)
  ) as b
from
  geultto_4th_prod.review_mapping
where
  -- UT3DE17S7 님은 훈련소 입소로 1회차 때 reviewee_ids 가 지정되지 않았습니다.
  not (user_id = 'UT3DE17S7' and due_ts = '2020-03-15 15:00:00 UTC' and insert_ts = '2020-03-01 16:00:00 UTC')
