import os
import pandas as pd
from utils import bigquery_config, phase, root_path

_project = bigquery_config[phase]['project']
_suffix = bigquery_config[phase]['suffix']
_jwt = os.path.join(root_path, 'config', bigquery_config[phase]['jwt'])

# TODO : 데이터 덤프한 후, 후처리 코드 작성 필요
# BigQuery Sample Code
# TODO : 아래 코드를 참고해 빅쿼리에서 데이터를 불러오고, 데이터를 넣을 수 있음 (샘플 코드)
query = f"SELECT * FROM `{_project}.slack_log.3rd_{_suffix}`"
df = pd.read_gbq(query=query, project_id=_project,
                 dialect='standard', private_key=_jwt)


sample_schema = [{'name': 'column1', 'type': 'STRING'},
                 {'name': 'column2', 'type': 'INTEGER'}]

df.to_gbq(destination_table=f'slack_log.3rd_{_suffix}',
          project_id=f'{_project}',
          if_exists='append', table_schema=sample_schema)
