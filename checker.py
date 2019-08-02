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
def check_deadline(deadline_str, time_str, d_type):
    '''
    string type로 입력된 deadline date (ex. 2019-07-22) 에 따라
    submit_deadline(월요일 새벽 두시), pass_deadline(일요일 자정)으로
    유효성 check
    '''
    deadline_time = datetime.strptime(deadline_str, '%Y-%m-%d')
    # sunday 12am
    pass_deadline = deadline_time
    # monday 2am
    submit_deadline = deadline_time + timedelta(hours=2)

    time = datetime.strptime(time_str, '%Y-%m-%d %H:%M:%S')

    if d_type == 'submit':
        return time < submit_deadline
    elif d_type == 'pass':
        return time < pass_deadline

