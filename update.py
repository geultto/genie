import datetime
import os
from functools import reduce
from math import ceil
from random import shuffle
from typing import List

import pandas as pd
from google.cloud import bigquery
from google.cloud.bigquery import QueryJobConfig
from google.cloud.bigquery.job import WriteDisposition
from slacker import Slacker

SLACK_CLIENT = Slacker(os.environ['GEULTTO_SLACK_TOKEN'])

# json key file 은 https://geultto4.slack.com/archives/GUT4CBFU6/p1583549101010400 에서 확인할 수 있습니다.
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'service_account.json'
BIGQUERY_CLIENT = bigquery.Client()

# CTPJHL1H8 3_데분데사a
# CTPJHM0GJ 3_데분데사b
# CU2C9MB96 3_데이터엔지니어
# CU4882QUF 3_백엔드a_인프라
# CTS8D3FFT 3_백엔드b_보안
# CU2CW80KF 3_안드로이드
# CU2CW93M3 3_프론트a
# CTPJJ07UJ 3_프론트b
CHANNEL_IDS = ['CTPJHL1H8', 'CTPJHM0GJ', 'CU2C9MB96', 'CU4882QUF', 'CTS8D3FFT', 'CU2CW80KF', 'CU2CW93M3', 'CTPJJ07UJ']


def read_sql(file_path):
    with open(file_path, 'r') as file:
        s = file.read()
    return s


def dict_to_message(channel_id, d):
    return {
        'channel_id': channel_id,
        'ts': d['ts'],
        'user_id': d['user'],
        'reactions': [{'name': r['name'], 'user_ids': r['users']} for r in d.get('reactions', [])],
        'parent_user_id': d.get('parent_user_id'),
        'thread_ts': d.get('thread_ts'),
        'text': d['text'],
        'client_msg_id': d.get('client_msg_id')
    }


def list_channel_messages(channel_id):
    print(f'list_channel_messages started for {channel_id}')
    channel_messages = []

    body = SLACK_CLIENT.conversations.history(channel=channel_id, limit=200).body
    assert body['ok'] and not body['has_more'], f'ok: {body["ok"]}, has_more: {body["has_more"]}'

    for message in body['messages']:
        # subtype 이 channel_topic, channel_join 인 메시지는 무시하고 None 인 경우 = 보통의 메시지만 취합니다.
        if message.get('type') == 'message' and not message.get('subtype'):
            if message.get('reply_count', 0) > 0:
                threads = SLACK_CLIENT.conversations.replies(channel=channel_id, ts=message['thread_ts']).body
                assert threads['ok'] and not threads['has_more'], f'{threads["ok"]}, {threads["has_more"]}'
                for thread in threads['messages']:
                    # threads 에는 thread 뿐 아니라 thread 가 달린 본래 message 까지 있으므로 걸러줍니다.
                    if not message['ts'] == thread['ts']:
                        channel_messages.append(dict_to_message(channel_id, thread))

            channel_messages.append(dict_to_message(channel_id, message))

    return channel_messages


def insert_message_raw():
    messages = reduce(lambda l1, l2: l1 + l2, [list_channel_messages(channel_id) for channel_id in CHANNEL_IDS])
    df = pd.DataFrame(messages)
    df['time_ms'] = int(datetime.datetime.now().timestamp() * 1000000)  # 언제 insert 했는지 epoch microseconds 로 적어줍니다.
    df.to_gbq(destination_table=f'geultto_4th_prod.message_raw', project_id='geultto', if_exists='append')


def update_message():
    sql = read_sql('sql/message_raw_to_message.sql')
    job_config = QueryJobConfig(destination='geultto.geultto_4th_prod.message',
                                write_disposition=WriteDisposition.WRITE_TRUNCATE)
    job = BIGQUERY_CLIENT.query(sql, job_config=job_config)
    job.result()  # async job 이라 result 를 명시적으로 호출해서 job 이 끝날때까지 blocking 합니다.


def get_reviewees(candidates: List[str], reviewers: List[str]) -> List[str]:
    multiple = ceil(len(reviewers) * 2 / len(candidates))
    reviewees = []

    for _ in range(multiple):
        members = candidates[:]
        shuffle(members)
        reviewees += members

    last_reviewer = reviewers[-1]
    i = len(reviewees) - 2

    if reviewees[i] == last_reviewer:
        n = len(candidates)
        reviewees[i], reviewees[-n] = reviewees[-n], reviewees[i]

    if reviewees[i + 1] == last_reviewer:
        n = len(candidates)
        reviewees[i + 1], reviewees[-n + 1] = reviewees[-n + 1], reviewees[i + 1]

    return reviewees


def swap_for_preventing_review_myself(reviewees: List[str], assignees: [str, str], cursor: int):
    duplicate = assignees[cursor]
    index = [index for index, reviewee in enumerate(reviewees) if reviewee != duplicate][0]

    assignees[cursor] = reviewees.pop(index)
    reviewees.insert(0, duplicate)


def swap_for_same_reviewee(reviewees: List[str], assignees: [str, str], reviewer: str):
    duplicate = assignees[0]
    index = [index for index, reviewee in enumerate(reviewees) if reviewee != duplicate and reviewee != reviewer][0]

    assignees[0] = reviewees.pop(index)
    reviewees.insert(0, duplicate)


def assign_reviewees(teams):
    assignments = []

    for team in teams.values():
        reviewers = team["reviewers"]

        if len(team["reviewees"]) <= 2:
            for reviewer in reviewers:
                assignments.append({"user_id": reviewer, "reviewee_ids": [reviewee for reviewee in team["reviewees"] if
                                                                          reviewee != reviewer]})
        else:
            reviewees = get_reviewees(team["reviewees"], reviewers)

            for reviewer in reviewers:
                assignees = reviewees[:2]
                draft_reviewees = reviewees[2:]

                while reviewer in assignees:
                    swap_for_preventing_review_myself(draft_reviewees, assignees, assignees.index(reviewer))

                while len(assignees) >= 2 and assignees[0] == assignees[1]:
                    swap_for_same_reviewee(draft_reviewees, assignees, reviewer)

                assignments.append({"user_id": reviewer, "reviewee_ids": assignees})
                reviewees = draft_reviewees

    return assignments


def insert_review_mapping():
    row_iterator = BIGQUERY_CLIENT.query(read_sql('sql/need_review_mapping_insert.sql')).result()
    assert row_iterator.total_rows == 1
    need_insert = list(row_iterator)[0].get('need_insert')
    assert isinstance(need_insert, bool)

    print(f'need_insert : {need_insert}')

    if need_insert:
        df = pd.read_gbq(query=read_sql('sql/reviewers_and_reviewees.sql'))
        teams = {}
        for row in df.itertuples():
            teams[row.channel_id] = {'reviewers': list(row.reviewers), 'reviewees': list(row.reviewees)}
        assignments = assign_reviewees(teams)
        # TODO timezone explicit 하게 명시.
        suffix = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
        table_review_mapping_raw = f'geultto_4th_staging.review_mapping_raw_{suffix}'
        df = pd.DataFrame(assignments)
        df['time_ms'] = int(datetime.datetime.now().timestamp() * 1000000)  # epoch microseconds.
        df.to_gbq(table_review_mapping_raw)
        BIGQUERY_CLIENT.query(read_sql('sql/review_mapping_raw_to_review_mapping.sql').format(
            table_review_mapping_raw=table_review_mapping_raw)).result()
        BIGQUERY_CLIENT.delete_table(table_review_mapping_raw)


def assert_sql(file_path):
    # sql 실행 결과가 b 라는 이름의 boolean column 으로 row 가 1개 뿐 이고 그 값이 true 여야 합니다.
    row_iterator = BIGQUERY_CLIENT.query(read_sql(file_path)).result()
    assert row_iterator.total_rows == 1
    b = list(row_iterator)[0].get('b')
    assert isinstance(b, bool)
    assert b, f'{file_path} 의 b 가 False.'


def assert_review_mapping():
    assert_sql('sql/assert_review_mapping_count_by_due.sql')
    assert_sql('sql/assert_reviewee_ids_predicates.sql')
    assert_sql('sql/assert_reviewers_are_equally_mapped.sql')


if __name__ == '__main__':
    # slack api 로 데이터 받아와서 message_raw 에 insert.
    insert_message_raw()

    # message_raw 에서 적절히 중복 제거하여 message 로 overwrite.
    update_message()

    # 필요하다면 reviewee 지정해서 review_mapping 에 insert.
    insert_review_mapping()

    # review_mapping 테이블의 데이터가 만족해야 할 것들을 확인합니다.
    assert_review_mapping()
