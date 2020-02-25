import yaml
import os

phase = 'development'

root_path = os.path.abspath(os.path.join(os.path.dirname(__file__), './'))

bigquery_config = yaml.load(
    open(
        os.path.join(root_path, 'config', 'bigquery.yaml'),
        'r'
    )
)


__all__ = ['phase', 'root_path', 'bigquery_config']


# CARDINAL_NUM
CARDINAL_NUM = 4

if CARDINAL_NUM == 3:
    cardinal = "3rd"
else:
    cardinal = str(CARDINAL_NUM) + "th"
