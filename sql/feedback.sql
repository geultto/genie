select
  due_ts, r.user_id,
  array_agg(
    struct(
      reviewee_id,
      user.user_name as reviewee_name,
      ts_min
    )
    order by user_name
  ) as feedbacks
from (
  select
    review_mapping.due_ts, review_mapping.user_id, reviewee_id,
    -- 같은 reviewee 에 대해 여러번 feedback 했을 수 있습니다.
    -- 가장 빨리한 ts 를 취한 뒤 due_ts 와 비교합니다.
    min(feedback.ts) as ts_min
  from
    geultto_5th_prod.review_mapping
    left join unnest(reviewee_ids) as reviewee_id
    left join (
      select
        feedback.due_ts, feedback.user_id, feedback.parent_user_id, feedback.ts
      from
        geultto_5th_prod.message feedback
        join geultto_5th_prod.message submit on feedback.channel_id = submit.channel_id
          and feedback.parent_user_id = submit.user_id
          and feedback.thread_ts = submit.ts
          and (select countif(r.name = 'submit' and submit.user_id in unnest(r.user_ids)) from unnest(submit.reactions) as r) > 0
      where
        (select countif(r.name = 'feedback' and feedback.user_id in unnest(r.user_ids)) from unnest(feedback.reactions) as r) > 0
    ) feedback on review_mapping.due_ts = feedback.due_ts
      and review_mapping.user_id = feedback.user_id
      and reviewee_id = feedback.parent_user_id
  group by
    due_ts, user_id, reviewee_id
) r
  left join geultto_5th_prod.user on reviewee_id = user.user_id
group by
  due_ts, user_id
order by
  due_ts, user_id
