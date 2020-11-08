WITH m AS (
  SELECT channel_id, FORMAT_TIMESTAMP("%F %T", ts, "Asia/Seoul") ts, user_id, parent_user_id, text,
--  # TODO : 수정
    CASE WHEN due_ts = '2020-11-15 15:00:00' THEN 1
         WHEN due_ts = '2020-11-29 15:00:00' THEN 2
         WHEN due_ts = '2020-12-13 15:00:00' THEN 3
         WHEN due_ts = '2020-12-27 15:00:00' THEN 4
         WHEN due_ts = '2021-01-10 15:00:00' THEN 5
         WHEN due_ts = '2021-01-24 15:00:00' THEN 6
         WHEN due_ts = '2021-02-07 15:00:00' THEN 7
         WHEN due_ts = '2021-02-21 15:00:00' THEN 8
         WHEN due_ts = '2021-03-07 15:00:00' THEN 9
         WHEN due_ts = '2021-03-21 15:00:00' THEN 10
         WHEN due_ts = '2021-04-04 15:00:00' THEN 11
         WHEN due_ts = '2021-04-18 15:00:00' THEN 12
         WHEN due_ts = '2021-05-02 15:00:00' THEN 13
    END round,
    CASE WHEN parent_user_id IS NULL AND ts < due_ts AND reaction.name = 'submit' THEN 'submit'
         WHEN parent_user_id IS NULL AND ts < due_ts AND reaction.name = 'pass' THEN 'pass'
         WHEN parent_user_id IS NOT NULL AND ts < due_ts AND reaction.name = 'feedback' THEN 'feedback'
    END reaction,
    CASE WHEN reaction.name = 'submit' THEN CONCAT("https://geultto4.slack.com/archives/", channel_id, "/p", UNIX_MICROS(ts), "/")
         WHEN reaction.name = 'feedback' THEN CONCAT("https://geultto4.slack.com/archives/", channel_id, "/p", UNIX_MICROS(thread_ts), "/")
    END m_url,
    REGEXP_EXTRACT_ALL(REGEXP_REPLACE(IF(reaction.name = 'submit', text, null), "\\|.+>", ">"), "<.+>") post_url
  FROM `geultto_5th_staging.message` , UNNEST(reactions) reaction
  WHERE reaction.name IN ('pass', 'feedback', 'submit') AND ts < insert_ts
)

SELECT u.channel_id, u.user_id, u.channel_name, u.user_name, round, m.ts, m.reaction, m.parent_user_id, m.m_url, m.post_url, m.text
FROM `geultto_5th_staging.user` u, UNNEST([1,2,3,4,5,6,7,8,9,10,11,12,13]) round
LEFT JOIN m ON u.user_id = m.user_id AND u.channel_id = m.channel_id AND round = m.round
;
