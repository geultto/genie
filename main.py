import argparse
from slacker import Slacker
import pandas as pd
from slack_export import *
from utils import bigquery_config, root_path
from checker import self_reaction_check, check_deadline, message_check, feedback_check, all_message_check
from extract_data import get_status_data, get_deadline_data, get_peer_reviewer_data
from extract_data import get_all_slack_log, get_all_status_board, get_all_users, get_all_messages

if __name__ == "__main__":
    # ---------------------------- save raw data from slack ---------------------------- #
    # It will generate data files in output Directory
    slack_token = os.getenv('SLACK_TOKEN')
    parser = argparse.ArgumentParser(description='Export Slack history')

    parser.add_argument('--token', default=slack_token, help="Slack API token")
    parser.add_argument('--zip', help="Name of a zip file to outputs as")
    parser.add_argument('--channel_prefix', default='3_', help="prefix of channel which need to be exported")
    parser.add_argument('--gbq_phase', default='development', help='BigQuery dealing phase: development / production')
    parser.add_argument('--if_exists_prod', default='append', help='BigQuery argument to deal with exisisting table: append / replace')
    parser.add_argument('--deadline', default='2019-08-26', help='deadline date (Monday): year-month-date')

    parser.add_argument(
        '--dry_run',
        action='store_true',
        default=False,
        help="List the conversations that will be exported (don't fetch/write history)")

    parser.add_argument(
        '--public_channels',
        nargs='*',
        default=None,
        metavar='CHANNEL_NAME',
        help="Export the given Public Channels")

    parser.add_argument(
        '--prompt',
        action='store_true',
        default=False,
        help="Prompt you to select the conversations to export")

    args = parser.parse_args()
    slack = Slacker(args.token)
    dry_run = args.dry_run
    zip_name = args.zip
    users, channels = bootstrap_key_values(slack, dry_run)

    output_directory = "outputs/{0}-slack_export".format(datetime.today().strftime("%Y%m%d-%H%M%S"))
    abs_output_directory = os.path.join(root_path, output_directory)

    mkdir(output_directory)
    os.chdir(output_directory)

    if not dry_run:
        dump_user_file(users)
        dump_channel_file(channels)

    selected_channels = select_conversations(
        channels,
        args.public_channels,
        filter_conversations_by_name,
        prompt_for_public_channels,
        args)

    if len(selected_channels) > 0:
        fetch_public_channels(selected_channels, args.channel_prefix)

    print('\nAll message data saved.\noutputs Directory: [%s]\n' % output_directory)

    finalize(zip_name, output_directory)


    # ---------------------------- Parameters for BigQuery ---------------------------- #
    phase = args.gbq_phase
    project_id = bigquery_config[phase]['project']            # geultto
    if phase == 'production':
        prod_table_suffix = bigquery_config[phase]['suffix']           # prod
        prod_log_table_id = 'slack_log.3rd_{}'.format(prod_table_suffix)
        prod_status_table_id = 'status_board.3rd_{}'.format(prod_table_suffix)
    table_suffix = bigquery_config['development']['suffix']           # staging
    log_table_id = 'slack_log.3rd_{}'.format(table_suffix)
    status_table_id = 'status_board.3rd_{}'.format(table_suffix)


    print('Getting data from BigQuery...')
    all_deadline_dates = get_deadline_data()
    # submit_num
    deadline_time = datetime.strptime(args.deadline, '%Y-%m-%d')
    submit_num = int(all_deadline_dates.loc[all_deadline_dates['date'] == deadline_time].index[0]) + 1
    # feedback check는 submit_num이 2 이상일 때부터 하므로
    # submit_num == 1일 때는 전체 데이터를 수집하지 않음
    if submit_num == 1:
        all_slack_log = None
        all_status_board = None
    else:
        all_slack_log = get_all_slack_log()
        all_status_board = get_all_status_board()
    # peer_reviewers
    peer_reviewers = get_peer_reviewer_data()
    print('Done.\n')


    # ---------------------------- Get users url data by reactions ---------------------------- #
    print('Now collecting user messages...')
    # Get user data
    users = get_all_users(abs_output_directory)

    # Get Filtered URLs from saved data
    # need dataz only from [3_*] channels
    all_channels = os.listdir(abs_output_directory)
    filtered_channels = sorted([i for i in all_channels if i.startswith(args.channel_prefix)])
    # collect all data from filtered channels
    all_messages = get_all_messages(filtered_channels, abs_output_directory)

    print('Done.\n')


    # ---------------------------- submit / pass / feedback check ---------------------------- #
    dataz = all_message_check(
        users,
        args.deadline,
        all_deadline_dates,
        peer_reviewers,
        submit_num,
        all_slack_log,
        all_status_board,
        all_messages)


    # -------------------- save data as pandas DataFrame & send to BigQuery -------------------- #
    print('All messages checked.\n\nSending Data to BigQuery...')

    # staging table에 새로운 데이터 전송
    dataz.to_gbq(log_table_id, project_id=project_id, if_exists='replace')
    # prod table에는 기존에 쌓인 데이터위에 새로운 데이터 추가
    if phase == 'production':
        dataz.to_gbq(prod_log_table_id, project_id=project_id, if_exists=args.if_exists_prod)


    status_df = get_status_data()
    # slack_log에 모두 저장된 데이터 바탕으로 query 날려서 새로 정렬하는 것이므로 항상 replace 인자 사용
    status_df.to_gbq(status_table_id, project_id=project_id, if_exists='replace')
    if phase == 'production':
        status_df.to_gbq(prod_status_table_id, project_id=project_id, if_exists=args.if_exists_prod)

    print('Succesfully sended.')
