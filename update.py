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
from slack_sdk import WebClient

slack_token = "xxx"

client = WebClient(token=slack_token)
# client = WebClient(token=os.environ['GEULTTO_SLACK_TOKEN'])
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'config/geultto-genie-46d7f46b7ca2.json'
BIGQUERY_CLIENT = bigquery.Client()


# 6기
# C02714U0S11 자기소개
# C028EANJBR9 3_데이터-분석가a
# C028B41AKTP 3_데이터-분석가b
# C028SPZHA2D 3_데이터-사이언티스트
# C028E4PJC66 3_데이터-엔지니어
# C028B41UP5K 3_딥러닝-리서치-사이언티스트a
# C027ZCVC0EB 3_딥러닝-리서치-사이언티스트b
# C027ZCVMQ5D 3_백엔드-개발a
# C0293R0P2GG 3_백엔드-개발b
# C028EAQ2KGT 3_백엔드-개발c
# C0287J5B1SS 3_백엔드-개발d
# C027ZCW4D71 3_인프라-devops
# C028B42NF45 3_클라이언트-개발
# C028LAQMNP6 3_프론트엔드-개발a
# C028SQ11V0R 3_프론트엔드-개발b
# C028SQ1509X 3_프론트엔드-개발c

CHANNEL_IDS = ['C028EANJBR9', 'C028B41AKTP', 'C028SPZHA2D', 'C028E4PJC66', 'C028B41UP5K','C027ZCVC0EB',
               'C027ZCVMQ5D', 'C0293R0P2GG', 'C028EAQ2KGT', 'C0287J5B1SS', 'C027ZCW4D71', 'C028B42NF45', 'C028LAQMNP6',
               'C028SQ11V0R', 'C028SQ1509X']


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

    body = client.conversations_history(channel=channel_id, limit=3000).data
    # TODO : 여기 1000개 가져오는거 나중에 문제될듯.. 점점 많은 데이터를 가져오게 한다
    print(body)
    # assert body['ok'] and not body['has_more'], f'ok: {body["ok"]}, has_more: {body["has_more"]}'

    for message in body['messages']:
        # subtype 이 channel_topic, channel_join 인 메시지는 무시하고 None 인 경우 = 보통의 메시지만 취합니다.
        if message.get('type') == 'message' and not message.get('subtype'):
            if message.get('reply_count', 0) > 0:
                threads = client.conversations_replies(channel=channel_id, ts=message['thread_ts'], limit=1000).data
                # print(threads)
                # assert threads['ok'] and not threads['has_more'], f'{threads["ok"]}, {threads["has_more"]}'
                for thread in threads['messages']:
                    # threads 에는 thread 뿐 아니라 thread 가 달린 본래 message 까지 있으므로 걸러줍니다.
                    if not message['ts'] == thread['ts']:
                        channel_messages.append(dict_to_message(channel_id, thread))

            channel_messages.append(dict_to_message(channel_id, message))

    return channel_messages


def insert_message_raw():
    # 만약 오류가 생겨서 하나에서만 진행한다면 아래와 같이 진행
    # CHANNEL_IDS = ['C01DMFUDE30']
    # CHANNEL_IDS = ['C01DR5EQ0GM']
    # CHANNEL_IDS = ['C01EJ3EH51N']
    # CHANNEL_IDS = ['C01EJ3D8W8G']
    # CHANNEL_IDS = ['C01DU81LTD0']
    # CHANNEL_IDS = ['C01DU82BP18']
    for channel_id in CHANNEL_IDS:
        print("channel_id", channel_id)
        messages = reduce(lambda l1, l2: l1 + l2, [list_channel_messages(channel_id)])
        # messages = reduce(lambda l1, l2: l1 + l2, [list_channel_messages(channel_id) for channel_id in CHANNEL_IDS])
        df = pd.DataFrame(messages)
        df['time_ms'] = int(datetime.datetime.now().timestamp() * 1000000)  # 언제 insert 했는지 epoch microseconds 로 적어줍니다.
        df.to_gbq(destination_table=f'geultto_6th_prod.message_raw', project_id='geultto', if_exists='append')


def update_table(sql, destination):
    job_config = QueryJobConfig(destination=destination, write_disposition=WriteDisposition.WRITE_TRUNCATE)
    # async job 이라 result 를 명시적으로 호출해서 job 이 끝날때까지 blocking 합니다.
    BIGQUERY_CLIENT.query(sql, job_config=job_config).result()


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
        print(df)
        teams = {}
        for row in df.itertuples():
            teams[row.channel_id] = {'reviewers': list(row.reviewers), 'reviewees': list(row.reviewees)}
        assignments = assign_reviewees(teams)
        # TODO timezone explicit 하게 명시.
        suffix = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
        table_review_mapping_raw = f'geultto_6th_prod.review_mapping_raw_{suffix}'
        df = pd.DataFrame(assignments)
        df['time_ms'] = int(datetime.datetime.now().timestamp() * 1000000)  # epoch microseconds.
        df.to_gbq(table_review_mapping_raw)
        BIGQUERY_CLIENT.query(read_sql('sql/review_mapping_raw_to_review_mapping.sql').format(
            table_review_mapping_raw=table_review_mapping_raw)).result()
        # BIGQUERY_CLIENT.delete_table(table_review_mapping_raw)


def assert_sql(file_path):
    # sql 실행 결과가 b 라는 이름의 boolean column 으로 row 가 1개 뿐 이고 그 값이 true 여야 합니다.
    row_iterator = BIGQUERY_CLIENT.query(read_sql(file_path)).result()
    assert row_iterator.total_rows == 1
    b = list(row_iterator)[0].get('b')
    assert isinstance(b, bool)
    assert b, f'{file_path} 의 b 가 False.'


def assert_review_mapping():
    # 아래 두개는 사실상 쓰지 않음
    # assert_sql('sql/assert_review_mapping_count_by_due.sql')
    # assert_sql('sql/assert_reviewee_ids_predicates.sql')
    assert_sql('sql/assert_reviewers_are_equally_mapped.sql')


def update_submit_table(destination):
    sql = read_sql('sql/create_submit_tbl.sql')
    job_config = QueryJobConfig(destination=destination, write_disposition=WriteDisposition.WRITE_TRUNCATE)
    job = BIGQUERY_CLIENT.query(sql, job_config=job_config)
    job.result()


if __name__ == '__main__':
    # # slack api 로 데이터 받아와서 message_raw 에 insert.
    insert_message_raw()
    # #
    # # # message_raw 에서 적절히 중복 제거하여 message 로 overwrite.
    update_table(read_sql('sql/message_raw_to_message.sql'), 'geultto.geultto_6th_prod.message')
    #
    # # 필요하다면 reviewee 지정해서 review_mapping 에 insert.
    insert_review_mapping()

    # review_mapping 테이블의 데이터가 만족해야 할 것들을 확인합니다.
    assert_review_mapping()

    # submit, pass, feedback 테이블 overwrite.
    # geultto_6th_prod
    update_table(read_sql('sql/submit.sql'), 'geultto.geultto_6th_prod.submit')
    update_table(read_sql('sql/pass.sql'), 'geultto.geultto_6th_prod.pass')
    update_table(read_sql('sql/feedback.sql'), 'geultto.geultto_6th_prod.feedback')
    update_table(read_sql('sql/result.sql'), 'geultto.geultto_6th_prod.result')
