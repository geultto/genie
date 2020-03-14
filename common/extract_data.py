import os, json
import pandas as pd
from datetime import datetime, timedelta
from utils import bigquery_config, phase, root_path, cardinal
from google.oauth2 import service_account

_project_id = bigquery_config[phase]['project']
_suffix = bigquery_config[phase]['suffix']
_jwt = os.path.join(root_path, 'config', bigquery_config[phase]['jwt'])

_credentials = service_account.Credentials.from_service_account_file(os.getenv('GOOGLE_APPLICATION_CREDENTIALS'))


def get_deadline_data(abs_output_directory):
    """
    feedback 위해 글또 3기의 제출 마감 날짜 목록 추출
    :return: dataframe, 데드라인의 날짜가 모두 저장된 데이터프레임
    """
    all_deadline_dates_df = pd.read_csv(os.path.join(abs_output_directory, "deadline_dates.csv"))
    all_deadline_dates_df["date"] = all_deadline_dates_df["date"].apply(lambda i: datetime.strptime(i, '%Y-%m-%d'))
    return all_deadline_dates_df


def get_peer_reviewer_data():
    """
    글또 3기의 피어 리뷰 목록 추출
    :return: dataframe, 6개월간의 모든 peer reviewer가 저장된 데이터프레임
    """
    query = f'''
    select *
    from `geultto.peer_reviewer.3rd_{_suffix}`
    '''
    peer_reviewer_df = pd.read_gbq(query, project_id=_project_id, dialect='standard', private_key=_jwt)
    peer_reviewer_df['date'] = peer_reviewer_df['date'].apply(lambda i: i.replace('.', '-'))
    peer_reviewer_df['date'] = peer_reviewer_df['date'].apply(lambda i: datetime.strptime(i, '%Y-%m-%d'))
    return peer_reviewer_df


def get_all_slack_log():
    """
    최소 두 번째 글부터 쌓여있는 prod_slack_log table 모두 추출
    :return: DataFrame
    """
    query = f'''
    select *
    from `geultto.slack_log.{cardinal}_prod`
    '''
    slack_log_df = pd.read_gbq(query, project_id=_project_id, dialect='standard', private_key=_jwt)
    return slack_log_df


def get_all_status_board():
    """
    prod_status_board table 모두 추출
    :return: DataFrame
    """
    query = f'''
    select *
    from `geultto.status_board.{cardinal}_prod`
    '''
    status_board_df = pd.read_gbq(query, project_id=_project_id, dialect='standard', private_key=_jwt)
    return status_board_df


def get_status_data():
    """
    status_table에 원하는 데이터만 추출해서 생성
    :return: status_df
    """
    query = f'''
    select name, team, submit_num, same_team_reviewer, other_team_reviewer, time, url, timestamp
    from `geultto.slack_log.{cardinal}_{_suffix}` as l left outer join `geultto.user_db.team_member` as r
    on l.user_id = r.id
    where name IS NOT NULL
    group by name, team, submit_num, time, timestamp, url, same_team_reviewer, other_team_reviewer
    order by submit_num, name
    '''
    status_df = pd.read_gbq(query, project_id=_project_id, dialect='standard', private_key=_jwt)
    return status_df


def get_all_users(abs_output_directory):
    """
    all_users를 {user_id: user_name} 형태로 추출
    :return: dict
    """
    users = {}
    with open(os.path.join(abs_output_directory, 'users.json')) as json_file:
        user_json = json.load(json_file)
        for user in user_json:
            if not (user['is_bot'] or (user['name'] == 'slackbot')):
                if 'real_name' in user.keys():
                    users[user['id']] = user['real_name']
                else:
                    users[user['id']] = user['profile']['real_name']
    return users


def get_all_messages(filtered_channels, abs_slack_export_directory):
    """
    원하는 채널의 all_messages 추출
    :return: list
    """
    all_messages = []
    for channel in filtered_channels:
        channel_path = os.path.join(abs_slack_export_directory, channel)
        for date in sorted(os.listdir(channel_path)):
            with open(os.path.join(channel_path, date)) as json_file:
                json_data = json.load(json_file)
                all_messages.extend(json_data)
    with open(os.path.join(abs_slack_export_directory, 'all_messages.json'), 'w') as out_file:
        json.dump(all_messages, out_file, indent=4)
    return all_messages


def send_data_to_gbq(dataz, phase, project_id, log_table_id, status_table_id, prod_log_table_id, prod_status_table_id, if_exists_prod):
    # staging table에 새로운 데이터 전송
    dataz.to_gbq(log_table_id, project_id=project_id, if_exists='replace')
    # prod table에는 기존에 쌓인 데이터위에 새로운 데이터 추가
    if phase == 'production':
        dataz.to_gbq(prod_log_table_id, project_id=project_id, if_exists=if_exists_prod)
    status_df = get_status_data()
    # slack_log에 모두 저장된 데이터 바탕으로 query 날려서 새로 정렬하는 것이므로 항상 replace 인자 사용
    status_df.to_gbq(status_table_id, project_id=project_id, if_exists='replace')
    if phase == 'production':
        status_df.to_gbq(prod_status_table_id, project_id=project_id, if_exists=if_exists_prod)


def write_user_table(table_name, user_table):
    user_data_frame = pd.DataFrame(data=user_table)
    user_data_frame.to_gbq(table_name, if_exists='replace', credentials=_credentials)


def read_user_table(table_name):
    query = 'select user_id, user_name, channel_id, channel_name from {}'.format(table_name)
    print(pd.read_gbq(query=query, credentials=_credentials))
