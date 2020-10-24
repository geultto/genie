select
  user_id, date_kr_due, user_name, deposit_deduction, pass, submit, reviewee_name1, feedback1, reviewee_name2, feedback2,
from (
  select
    user_id, date_kr_due, user_name, pass, submit, reviewee_name1, feedback1, reviewee_name2, feedback2,
    deposit_deduction_post + deposit_deduction_feedback1 + deposit_deduction_feedback2 as deposit_deduction,
  from (
    select
      user_id, date_sub(date(due_ts, 'Asia/Seoul'), interval 1 day) as date_kr_due, user_name,
      case
        when pass.is_valid then 'PASS 적용 O'
        when not pass.is_valid and pass.is_successive then 'PASS 적용 X (연속 사용)'
        when not pass.is_valid and ordinal > 2 then 'PASS 적용 X (2회 초과)'
        else 'PASS 사용 X'
      end as pass,
      case
        when submit_submit.is_valid then '글 제출 O'
        else '글 제출 X'
      end as submit,
      feedbacks[safe_offset(0)].reviewee_name as reviewee_name1,
      case
        when feedbacks[safe_offset(0)].reviewee_id is null then null
        when feedbacks[safe_offset(0)].ts_min < due_ts then '피드백 O'
        when feedbacks[safe_offset(0)].ts_min >= due_ts then '피드백 늦음'
        when feedbacks[safe_offset(0)].ts_min is null then '피드백 X'
      end as feedback1,
      feedbacks[safe_offset(1)].reviewee_name as reviewee_name2,
      case
        when feedbacks[safe_offset(1)].reviewee_id is null then null
        when feedbacks[safe_offset(1)].ts_min < due_ts then '피드백 O'
        when feedbacks[safe_offset(1)].ts_min >= due_ts then '피드백 늦음'
        when feedbacks[safe_offset(1)].ts_min is null then '피드백 X'
      end as feedback2,
      if(pass.is_valid or submit_submit.is_valid, 0, -5000) as deposit_deduction_post,
      if(pass.is_valid or feedbacks[safe_offset(0)].reviewee_id is null or feedbacks[safe_offset(0)].ts_min < due_ts, 0, -2500) as deposit_deduction_feedback1,
      if(pass.is_valid or feedbacks[safe_offset(1)].reviewee_id is null or feedbacks[safe_offset(1)].ts_min < due_ts, 0, -2500) as deposit_deduction_feedback2,
    from (
      select
        distinct(due_ts) as due_ts
      from
        geultto_4th_prod.message
      where
        due_ts < current_timestamp()
    ) due
      cross join geultto_4th_prod.user
      left join geultto_4th_prod.pass using (due_ts, user_id)
      left join geultto_4th_prod.submit_submit using (due_ts, user_id)
      left join geultto_4th_prod.feedback using (due_ts, user_id)
  )
)
order by
  date_kr_due, user_name
