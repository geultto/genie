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
