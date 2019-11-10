import re
import pandas as pd
from datetime import datetime, timedelta


def self_reaction_check(check_reaction, message, self_check=True):
    '''
    메세지에 self로 check_reaction을 입력했는지 확인
    '''
    reactions = message['reactions']
    user_id = message['user']
    for reaction in reactions:
        if reaction['name'] == check_reaction:
            if not self_check:
                return True
            else:
                if user_id in reaction['users']:
                    return True
    return False


# deadline date check
def check_deadline(deadline, time, all_deadline_dates, d_type, thread_ts=None):
    '''
    string type로 입력된 deadline date (ex. 2019-07-22) 에 따라
    submit_deadline(월요일 새벽 두시), pass_deadline(일요일 자정)으로
    유효성 check
    '''
    if type(deadline) == str:
        deadline = datetime.strptime(deadline, '%Y-%m-%d')

    # all_deadline_dates에서 현재 deadline의 index
    current_deadline_index = all_deadline_dates.loc[all_deadline_dates['date'] == deadline].index

    # 만약 첫 글이면: previous_deadline이 없음
    if current_deadline_index == 0:
        previous_deadline = None
        pass_previous_deadline = None
        submit_previous_deadline = None
    # 첫 글 외에는 current_deadline_index를 이용해 previous_deadline 날짜 추출
    else:
        previous_deadline = all_deadline_dates.loc[current_deadline_index - 1].values[0][0]
        previous_deadline = pd.to_datetime(previous_deadline) + timedelta(hours=2)

    # pass deadline : 12am / submit deadline : 2am
    pass_deadline = deadline
    submit_deadline = deadline + timedelta(hours=2)
    if previous_deadline:
        pass_previous_deadline = previous_deadline
        submit_previous_deadline = previous_deadline + timedelta(hours=2)
    # time = datetime.strptime(time_str, '%Y-%m-%d %H:%M:%S')

    if d_type == 'submit':
        if submit_previous_deadline:
            return True if (submit_previous_deadline < time) and (time < submit_deadline) else False
        else:
            return time < submit_deadline
    elif d_type == 'pass':
        if pass_previous_deadline:
            return True if (pass_previous_deadline < time) and (time < pass_deadline) else False
        else:
            return time < pass_deadline
    elif d_type == 'feedback':
        if current_deadline_index > 0:
            # 첫 피드백(두번째마감)이면: 피드백 달아야 할 글은 previous_deadline이 없음
            if current_deadline_index == 1:
                pre_previous_deadline = None
            else:
                pre_previous_deadline = all_deadline_dates.loc[current_deadline_index - 2].values[0][0]
                pre_previous_deadline = pd.to_datetime(pre_previous_deadline) + timedelta(hours=2)
            # 제출된 글이 저번 마감에 쓰인 글이고, 내 피드백이 이번 마감 전이면 return True
            if pre_previous_deadline:
                thread_is_in_last_deadline = (pre_previous_deadline < thread_ts) and (thread_ts < previous_deadline)
                feedback_is_in_deadline = time < submit_deadline
                return thread_is_in_last_deadline and feedback_is_in_deadline
            else:
                thread_is_in_last_deadline = thread_ts < previous_deadline
                feedback_is_in_deadline = time < submit_deadline
                return thread_is_in_last_deadline and feedback_is_in_deadline

def message_check(message, dataz, users, peer_reviewers, submit_num, all_slack_log, check_reaction):
    time = str(datetime.fromtimestamp(float(message['ts'])))[:-7]
    if check_reaction == 'submit':
        if (('attachments' in message.keys()) or ('<https://' in message['text'])) and ('reactions' in message.keys()):
            if self_reaction_check(check_reaction, message):
                message_id = message['client_msg_id']
                user_id = message['user']
                if 'attachments' in message.keys():
                    link = message['attachments'][0]['title_link']
                # naver blog의 경우 attachments로 생성 안되어 regex로 잡기
                elif '<https://' in message['text']:
                    message_text = message['text']
                    pattern = re.compile('<https://.+?>')
                    link = pattern.search(message_text)
                    link = link.group()[1:-1]
                else:
                    link = 'Link: Submitted but not Found, check required.'
                dataz.loc[dataz['user_id'] == user_id, 'url'] = link
                dataz.loc[dataz['user_id'] == user_id, 'time'] = time
                dataz.loc[dataz['user_id'] == user_id, 'deadline_check'] = True
                dataz.loc[dataz['user_id'] == user_id, 'message_id'] = message_id

    elif check_reaction == 'pass':
        if ('reactions' in message.keys()) and (self_reaction_check(check_reaction, message)):
            message_id = message['client_msg_id']
            user_id = message['user']
            dataz.loc[dataz['user_id'] == user_id, 'url'] = 'pass'
            dataz.loc[dataz['user_id'] == user_id, 'time'] = time
            dataz.loc[dataz['user_id'] == user_id, 'deadline_check'] = True
            dataz.loc[dataz['user_id'] == user_id, 'message_id'] = message_id

    elif check_reaction == 'feedback':
        if ('reactions' in message.keys()) and (self_reaction_check(check_reaction, message)):
            # reviewer: 피드백을 해 준 사람
            user_id = message['user']
            reviewer_name = users[user_id]
            # rewiewer가 이번에 피드백 해주어야 할 두명
            two_reviewers = peer_reviewers.loc[peer_reviewers['name'] == reviewer_name]
            two_reviewers = two_reviewers.loc[two_reviewers['submit_num'] == submit_num-1]
            two_reviewers = two_reviewers[['same_team_reviewer', 'other_team_reviewer']].values[0]
            # writer: 피드백 받은 글을 쓴 사람
            writer_name = users[message['parent_user_id']]
            if writer_name == two_reviewers[0]:
                dataz.loc[dataz['user_id'] == user_id, 'same_team_reviewer'] = f'{writer_name}_1'
            elif writer_name == two_reviewers[1]:
                dataz.loc[dataz['user_id'] == user_id, 'other_team_reviewer'] = f'{writer_name}_1'


def feedback_other(dataz, users, peer_reviewers, submit_num, all_status_board):
    for user_id, user_name in users.items():
        two_reviewers = peer_reviewers.loc[peer_reviewers['name'] == user_name]
        two_reviewers = two_reviewers.loc[two_reviewers['submit_num'] == submit_num-1]
        two_reviewers = two_reviewers[['same_team_reviewer', 'other_team_reviewer']].values
        if len(two_reviewers) > 0:
            two_reviewers = two_reviewers[0]
        # 글을 안 쓴 경우 feedback도 pass
        url_status = dataz.loc[dataz['user_id'] == user_id, 'url'].values
        if url_status == -1:
            dataz.loc[dataz['user_id'] == user_id, 'same_team_reviewer'] = 'no post'
            dataz.loc[dataz['user_id'] == user_id, 'other_team_reviewer'] = 'no post'
        # 글을 pass한 경우 feedback도 pass
        elif url_status == 'pass':
            dataz.loc[dataz['user_id'] == user_id, 'same_team_reviewer'] = 'pass post'
            dataz.loc[dataz['user_id'] == user_id, 'other_team_reviewer'] = 'pass post'
        else:
            for idx, writer_name in enumerate(two_reviewers):
                previous_submit = all_status_board.loc[all_status_board['submit_num'] == submit_num-1]
                previous_submit = previous_submit.loc[previous_submit['name'] == writer_name, 'url'].values[0]
                if previous_submit in ['pass', '-1']:
                    if idx == 0:
                        dataz.loc[dataz['user_id'] == user_id, 'same_team_reviewer'] = f'{writer_name}_{previous_submit}'
                    elif idx == 1:
                        dataz.loc[dataz['user_id'] == user_id, 'other_team_reviewer'] = f'{writer_name}_{previous_submit}'
        if submit_num == 0:
            dataz.loc[dataz['user_id'] == user_id, 'same_team_reviewer'] = None
            dataz.loc[dataz['user_id'] == user_id, 'other_team_reviewer'] = None


def all_message_check(users, deadline, all_deadline_dates, peer_reviewers, submit_num, \
                      all_slack_log, all_status_board, all_messages):
    submit_reaction = 'submit'
    pass_reaction = 'pass'
    feedback_reaction = 'feedback'

    dataz = pd.DataFrame({'user_id': userid,
                          'submit_num': submit_num,
                          'url': -1,
                          'time': -1,
                          'deadline_check': None,
                          'message_id': None,
                          'same_team_reviewer': '-0.25',
                          'other_team_reviewer': '-0.25'}
                          for userid in users)

    for message in all_messages:
        time = datetime.fromtimestamp(float(message['ts']))

        # deadline 안에 있는 message만 검사
        # 1) submit
        is_in_submit_deadline = check_deadline(deadline, time, all_deadline_dates, submit_reaction)
        if is_in_submit_deadline:
            message_check(message, dataz, users, peer_reviewers, submit_num, all_slack_log, check_reaction='submit')

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

    # feedback을 받아야 하는 사람이 글을 안 쓴 경우 (pass / -1 인 경우)
    # pass_{name} 또는 -1_{name} 으로 입력
    feedback_other(dataz, users, peer_reviewers, submit_num, all_status_board)

    return dataz
