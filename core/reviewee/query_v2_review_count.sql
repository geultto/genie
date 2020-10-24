select
  reviewer.channel_id,
  reviewer.user_id as reviewer,
  reviewee.user_id as reviewee,
  ifnull(cnt, 0) as cnt
from
  geultto_4th_prod.user reviewer
  join geultto_4th_prod.user reviewee on reviewer.channel_id = reviewee.channel_id
    and not reviewer.user_id = reviewee.user_id
  left join (
    select
      user_id, parent_user_id, count(1) as cnt
    from
      geultto_4th_prod.message
    where
      (select countif(r.name = 'feedback') from unnest(reactions) as r) > 0
      and parent_user_id is not null
    group by
      user_id, parent_user_id
  ) f on reviewer.user_id = f.user_id
    and reviewee.user_id = f.parent_user_id