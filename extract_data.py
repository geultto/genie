import os
import pandas as pd
from datetime import datetime, timedelta
from utils import bigquery_config, phase, root_path

_project_id = bigquery_config[phase]['project']
_suffix = bigquery_config[phase]['suffix']
_jwt = os.path.join(root_path, 'config', bigquery_config[phase]['jwt'])


def get_deadline_data():
    """
    feedback 위해 글또 3기의 제출 마감 날짜 목록 추출
    :return: dataframe, 데드라인의 날짜가 모두 저장된 데이터프레임
    """
    query = f'''
    select date
    from `geultto.peer_reviewer.3rd_{_suffix}`
    group by date
    '''
    all_deadline_dates_df = pd.read_gbq(query, project_id=_project_id, dialect='standard', private_key=_jwt)
    all_deadline_dates_df['date'] = all_deadline_dates_df['date'].apply(lambda i: i.replace('.', '-'))
    all_deadline_dates_df['date'] = all_deadline_dates_df['date'].apply(lambda i: datetime.strptime(i, '%Y-%m-%d'))
    return all_deadline_dates_df


def get_status_data():
    """
    status_table에 원하는 데이터만 추출해서 생성
    :return: status_df
    """
    query = f'''
    select name, team, submit_num, time, url
    from `geultto.slack_log.3rd_{_suffix}` as l left outer join `geultto.user_db.team_member` as r
    on l.user_id = r.id
    where name IS NOT NULL
    group by name, team, submit_num, time, url
    order by submit_num, name
    '''
    status_df = pd.read_gbq(query, project_id=_project_id, dialect='standard', private_key=_jwt)
    return status_df
