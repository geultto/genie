import pandas as pd

from slacker import Slacker


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


token = '<token>'
channel_ids = [
    'CTPJHL1H8',  # 3_데분데사a
    'CTPJHM0GJ',  # 3_데분데사b
    'CU2C9MB96',  # 3_데이터엔지니어
    'CU4882QUF',  # 3_백엔드a_인프라
    'CTS8D3FFT',  # 3_백엔드b_보안
    'CU2CW80KF',  # 3_안드로이드
    'CU2CW93M3',  # 3_프론트a
    'CTPJJ07UJ'  # 3_프론트b
]

client = Slacker(token)

messages = []

for channel_id in channel_ids:
    print(channel_id)
    body = client.conversations.history(channel=channel_id, limit=100).body
    assert body['ok'] and not body['has_more'], f'ok: {body["ok"]}, has_more: {body["has_more"]}'

    for message in body['messages']:
        # subtype 이 channel_topic, channel_join 인 메시지는 무시하고 None 인 경우 = 보통의 메시지만 취합니다.
        if message.get('type') == 'message' and not message.get('subtype'):
            if message.get('reply_count', 0) > 0:
                threads = client.conversations.replies(channel=channel_id, ts=message['thread_ts'], limit=100).body
                assert threads['ok'] and not threads['has_more'], f'{threads["ok"]}, {threads["has_more"]}'
                for thread in threads['messages']:
                    # threads 에는 thread 뿐 아니라 thread 가 달린 본래 message 까지 있으므로 걸러줍니다.
                    if not message['ts'] == thread['ts']:
                        messages.append(dict_to_message(channel_id, thread))

            messages.append(dict_to_message(channel_id, message))

df = pd.DataFrame(messages)
df.to_gbq(destination_table='geultto_4th_staging.message_raw', project_id='geultto', if_exists='replace')
