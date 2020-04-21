import datetime
import os
from functools import reduce

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


def read_sql(file_name):
    with open(file_name, 'r') as file:
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

    body = SLACK_CLIENT.conversations.history(channel=channel_id).body
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


def insert_to_message_raw(messages):
    df = pd.DataFrame(messages)
    df['time_ms'] = int(datetime.datetime.now().timestamp() * 1000000)  # 언제 insert 했는지 epoch microseconds 로 적어줍니다.
    df.to_gbq(destination_table=f'geultto_4th_prod.message_raw', project_id='geultto', if_exists='append')


def update_message(destination):
    sql = read_sql('sql/message_raw_to_message.sql')
    job_config = QueryJobConfig(destination=destination, write_disposition=WriteDisposition.WRITE_TRUNCATE)
    job = BIGQUERY_CLIENT.query(sql, job_config=job_config)
    job.result()  # async job 이라 result 를 명시적으로 호출해서 job 이 끝날때까지 blocking 합니다.

def update_submit_table(destination):
    sql = read_sql('sql/create_submit_tbl.sql')
    job_config = QueryJobConfig(destination=destination, write_disposition=WriteDisposition.WRITE_TRUNCATE)
    job = BIGQUERY_CLIENT.query(sql, job_config=job_config)
    job.result()

if __name__ == '__main__':
    messages = reduce(lambda l1, l2: l1 + l2, [list_channel_messages(channel_id) for channel_id in CHANNEL_IDS])
    insert_to_message_raw(messages)
    update_message('geultto.geultto_4th_prod.message')
    update_submit_table('geultto.geultto_4th_prod.submit')
