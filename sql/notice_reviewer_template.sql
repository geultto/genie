SELECT
  CONCAT(
    channel_name,
    '\n',
    """
*10회차 글 리뷰어 알림*
    - 지정된 리뷰어의 10회차 (2021/12/05) 사이에 제출된 글을 리뷰해주세요
    - 다음 글쓰기 마감일인 2021/12/19 23:59 까지 리뷰하면 됩니다 (기한을 넘기면 예치금이 차감돼요!)
    - 한 리뷰어당 한 개 이상의 피드백을 하면 됩니다 (한 리뷰어가 글을 여러 개 쓴 경우 1개 이상의 피드백만 하면 인정!)
    """,
    '\n',
    STRING_AGG(CONCAT('*', CONCAT(user_name, ' -> ', reviewee), '*', '\n', '- ', url, '\n'), '\n' ORDER BY user_name),
    '\n'
    )
FROM (
  SELECT
    u1.channel_name,
    u1.user_name,
    STRING_AGG(DISTINCT u2.user_name, ', ') reviewee,
    STRING_AGG(DISTINCT CONCAT(u2.user_name, ' (', submit.submit_m_url, ')'), '\n- ') url
  FROM (
    SELECT
      user_id,
      reviewee_id
    FROM
      geultto_6th_prod.review_mapping review,
      UNNEST(reviewee_ids) reviewee_id
    WHERE
      due_ts = '2021-12-19 15:00:00 UTC'
  ) r
  LEFT JOIN `geultto_6th_prod.user` u1 ON r.user_id = u1.user_id
  LEFT JOIN `geultto_6th_prod.user` u2 ON r.reviewee_id = u2.user_id
  LEFT JOIN (
      select
        user_id,
        concat("https://geultto6.slack.com/archives/", channel_id, "/p", unix_micros(ts)) as submit_m_url
      from
        geultto_6th_prod.message
      where
        (select countif(reaction.name = 'submit' and user_id in unnest(reaction.user_ids)) from unnest(reactions) as reaction) > 0
        and due_ts = '2021-12-05 15:00:00 UTC'
    ) submit ON r.reviewee_id = submit.user_id
  GROUP BY u1.channel_name, u1.user_name
  )
GROUP BY channel_name
ORDER BY channel_name
