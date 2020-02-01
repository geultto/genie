import sys, os
sys.path.append("../common")
from slacker import Slacker
from slack_export import *
from checker import *
from extract_data import *

deadline = "2019-12-09"
data_dir = "20191224-003201-slack_export"
abs_output_directory = os.path.join(root_path, '../outputs')
output_directory = os.path.join(abs_output_directory, data_dir)
abs_slack_export_directory = os.path.join(root_path, output_directory)
os.chdir(output_directory)

slack_token = os.getenv('SLACK_TOKEN')
slack = Slacker(slack_token)
users = get_all_users(abs_output_directory)
all_deadline_dates = get_deadline_data(abs_output_directory)
peer_reviewers = get_peer_reviewer_data()
deadline_time = datetime.strptime(deadline, '%Y-%m-%d')
submit_num = int(all_deadline_dates.loc[all_deadline_dates['date'] == deadline_time].index[0])
if submit_num == 0:
    all_slack_log = None
    all_status_board = None
else:
    all_slack_log = get_all_slack_log()
    all_status_board = get_all_status_board()

messages = [{
    "client_msg_id": "46959136-c285-4e23-94e2-b7577c3c38a5",
    "type": "message",
    "text": "\ucd5c\uadfc\uc5d0 \ud14c\uc2a4\ud2b8\uc5d0 \ub300\ud574\uc11c \uacf5\ubd80\ud558\uba74\uc11c\n\n*`JUnit 5 Parameterized Tests` \uc640 `\ud14c\uc2a4\ud2b8 \ucee4\ubc84\ub9ac\uc9c0\ub294 \ub192\uc744\uc218\ub85d \uc88b\uc744\uae4c?` \ub97c \uc815\ub9ac\ud574\ubd24\uc2b5\ub2c8\ub2e4.*\n&lt;https://velog.io/@dpudpu/JUnit-5-Parameterized-Tests-%EC%82%AC%EC%9A%A9%ED%95%98%EA%B8%B0-sjk3rfhqkg&gt;\n\n&lt;https://velog.io/@dpudpu/test-coverage&gt;",
    "user": "UKAG4RKSM",
    "ts": "1575822710.008000",
    "team": "TK42Y5SAW",
    "edited": {
        "user": "UKAG4RKSM",
        "ts": "1575824267.000000"
    },
    "blocks": [
        {
            "type": "rich_text",
            "block_id": "i=sQ",
            "elements": [
                {
                    "type": "rich_text_section",
                    "elements": [
                        {
                            "type": "text",
                            "text": "\ucd5c\uadfc\uc5d0 \ud14c\uc2a4\ud2b8\uc5d0 \ub300\ud574\uc11c \uacf5\ubd80\ud558\uba74\uc11c\n\n"
                        },
                        {
                            "type": "text",
                            "text": "JUnit 5 Parameterized Tests",
                            "style": {
                                "bold": True,
                                "code": True
                            }
                        },
                        {
                            "type": "text",
                            "text": " \uc640 ",
                            "style": {
                                "bold": True
                            }
                        },
                        {
                            "type": "text",
                            "text": "\ud14c\uc2a4\ud2b8 \ucee4\ubc84\ub9ac\uc9c0\ub294 \ub192\uc744\uc218\ub85d \uc88b\uc744\uae4c?",
                            "style": {
                                "bold": True,
                                "code": True
                            }
                        },
                        {
                            "type": "text",
                            "text": " \ub97c \uc815\ub9ac\ud574\ubd24\uc2b5\ub2c8\ub2e4.",
                            "style": {
                                "bold": True
                            }
                        },
                        {
                            "type": "text",
                            "text": "\n"
                        },
                        {
                            "type": "link",
                            "url": "https://velog.io/@dpudpu/JUnit-5-Parameterized-Tests-%EC%82%AC%EC%9A%A9%ED%95%98%EA%B8%B0-sjk3rfhqkg"
                        },
                        {
                            "type": "text",
                            "text": "\n\n"
                        },
                        {
                            "type": "link",
                            "url": "https://velog.io/@dpudpu/test-coverage"
                        }
                    ]
                }
            ]
        }
    ],
    "thread_ts": "1575822710.008000",
    "reply_count": 2,
    "reply_users_count": 2,
    "latest_reply": "1575995393.009000",
    "reply_users": [
        "UKJKH3KST",
        "UKAG4RKSM"
    ],
    "replies": [
        {
            "user": "UKJKH3KST",
            "ts": "1575851979.008800"
        },
        {
            "user": "UKAG4RKSM",
            "ts": "1575995393.009000"
        }
    ],
    "subscribed": False,
    "reactions": [
        {
            "name": "submit",
            "users": [
                "UKAG4RKSM"
            ],
            "count": 1
        },
        {
            "name": "+1",
            "users": [
                "UKJKH3KST"
            ],
            "count": 1
        }
    ]
}]

submit_reaction = 'submit'
pass_reaction = 'pass'
feedback_reaction = 'feedback'

dataz = pd.DataFrame({'user_id': userid,
                      'submit_num': submit_num,
                      'url': -1,
                      'time': -1,
                      'timestamp': -1,
                      'deadline_check': None,
                      'message_id': None,
                      'same_team_reviewer': '-0.25',
                      'other_team_reviewer': '-0.25'}
                      for userid in users)

for message in messages:
    time = datetime.fromtimestamp(float(message['ts']))

    # deadline 안에 있는 message만 검사
    # 1) submit
    is_in_submit_deadline = check_deadline(deadline, time, all_deadline_dates, submit_reaction)
    if is_in_submit_deadline:
        print("is in submit deadline")
        submit_time = str(datetime.fromtimestamp(float(message['ts'])))[:-7]
        if (('attachments' in message.keys()) or ('https://' in message['text'])) and ('reactions' in message.keys()):
            print("has attachment and reaction")
            if self_reaction_check("submit", message):
                print("self reactioned")
                message_id = message['client_msg_id']
                user_id = message['user']
                if ('attachments' in message.keys()) and ('title_link' in message['attachments'][0].keys()):
                    link = message['attachments'][0]['title_link']
                # naver blog의 경우 attachments로 생성 안되어 regex로 잡기
                elif 'https://' in message['text']:
                    message_text = message['text']
                    pattern = re.compile('<https://.+?>')
                    link = pattern.search(message_text)
                    if not link:
                        pattern = re.compile('https://.+?&')
                        link = pattern.search(message_text)
                        link = link.group()[:-1]
                    else:
                        link = link.group()[1:-1]
                    print(link)
                else:
                    link = 'Link: Submitted but not Found, check required.'
                dataz.loc[dataz['user_id'] == user_id, 'url'] = link
                dataz.loc[dataz['user_id'] == user_id, 'time'] = submit_time
                dataz.loc[dataz['user_id'] == user_id, 'timestamp'] = float(message['ts'])
                dataz.loc[dataz['user_id'] == user_id, 'deadline_check'] = True
                dataz.loc[dataz['user_id'] == user_id, 'message_id'] = message_id

    # 2) pass
    is_in_pass_deadline = check_deadline(deadline, time, all_deadline_dates, pass_reaction)
    if is_in_pass_deadline:
        message_check(message, dataz, users, peer_reviewers, submit_num, all_slack_log, check_reaction='pass')

    # 3) feedback
    # feedback은 댓글로 달린 메세지만 체크: thread_ts값 있는 메세지만 체크
    if 'thread_ts' in message.keys():
        thread_ts = datetime.fromtimestamp(float(message['thread_ts']))
        is_in_feedback_deadline = check_deadline(deadline, time, all_deadline_dates, feedback_reaction, thread_ts=thread_ts)
        # feedback은 두 번째 이후부터 체크
        if is_in_feedback_deadline and (submit_num > 0):
            message_check(message, dataz, users, peer_reviewers, submit_num, all_slack_log, check_reaction='feedback')

print(dataz[dataz["user_id"] == "UKAG4RKSM"])
