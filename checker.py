import pandas as pd
from datetime import datetime, timedelta


def self_reaction_check(check_reaction, message):
    '''
    메세지에 self로 check_reaction을 입력했는지 확인
    '''
    reactions = message['reactions']
    user_id = message['user']
    for reaction in reactions:
        if reaction['name'] == check_reaction:
            if user_id in reaction['users']:
                return True
    return False


# deadline date check
def check_deadline(deadline_str, time_str, all_deadline_dates, d_type):
    '''
    string type로 입력된 deadline date (ex. 2019-07-22) 에 따라
    submit_deadline(월요일 새벽 두시), pass_deadline(일요일 자정)으로
    유효성 check
    '''
    deadline_time = datetime.strptime(deadline_str, '%Y-%m-%d')

    # all_deadline_dates에서 현재 deadline의 index
    current_deadline_index = all_deadline_dates.loc[all_deadline_dates['date'] == deadline_time].index

    # 만약 첫 글이면: previous_deadline이 없음
    if current_deadline_index == 0:
        previous_deadline = None
    # 첫 글 외에는 current_deadline_index를 이용해 previous_deadline 날짜 추출
    else:
        previous_deadline = all_deadline_dates.loc[current_deadline_index - 1].values[0][0]
        previous_deadline = pd.to_datetime(previous_deadline)

    # sunday 12am
    pass_deadline = deadline_time
    pass_previous_deadline = previous_deadline
    # monday 2am
    submit_deadline = deadline_time + timedelta(hours=2)
    if previous_deadline:
        submit_previous_deadline = previous_deadline + timedelta(hours=2)
    else:
        submit_previous_deadline = previous_deadline

    time = datetime.strptime(time_str, '%Y-%m-%d %H:%M:%S')

    if d_type == 'submit':
        if not submit_previous_deadline:
            return time < submit_deadline
        else:
            return True if (submit_previous_deadline < time) and (time < submit_deadline) else False
    elif d_type == 'pass':
        if not pass_previous_deadline:
            return time < pass_deadline
        else:
            return True if (pass_previous_deadline < time) and (time < pass_deadline) else False
