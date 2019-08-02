import pandas as pd
from utils import bigquery_config, phase

_project_id = bigquery_config[phase]['project']
_suffix = bigquery_config[phase]['suffix']


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
    all_deadline_dates_df = pd.read_gbq(query, project_id=_project_id, dialect='standard')
    return all_deadline_dates_df


def get_status_data():
    """
    status_table에 원하는 데이터만 추출해서 생성
    :return: status_df
    """
    query = f'''
    select name, team, time, url
    from `geultto.slack_log.3rd_{_suffix}` as l left outer join `geultto.user_db.team_member` as r
    on l.userId = r.id
    where name IS NOT NULL
    '''
    status_df = pd.read_gbq(query, project_id=_project_id, dialect='standard')
    return status_df
