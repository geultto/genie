import argparse, time
from slacker import Slacker
import pandas as pd
from slack_export import *
from utils import bigquery_config, root_path
from checker import all_message_check
from extract_data import get_status_data, get_deadline_data, get_peer_reviewer_data, send_data_to_gbq
from extract_data import get_all_slack_log, get_all_status_board, get_all_users, get_all_messages

if __name__ == "__main__":
    # ---------------------------- save raw data from slack ---------------------------- #
    # It will generate data files in output Directory
    slack_token = os.getenv('SLACK_TOKEN')
    parser = argparse.ArgumentParser(description='Export Slack history')

    parser.add_argument('--token', default=slack_token, help="Slack API token")
    parser.add_argument('--zip', help="Name of a zip file to outputs as")
    parser.add_argument('--channel_prefix', default='3_', help="prefix of channel which need to be exported")
    parser.add_argument('--gbq_phase', default='production', help='BigQuery dealing phase: development / production')
    parser.add_argument('--run_all', default=False, help='run all deadlines before / run only this submission')
    parser.add_argument('--deadline', required=True, help='deadline date (Monday): yyyy-mm-dd')
    parser.add_argument('--data_dir',  default=False, help='data dir to get all messages')
    parser.add_argument('--dump_id_files', action='store_true', default=False, help="restore user/channnel ids")
    parser.add_argument('--public_channels', nargs='*', default=None, metavar='CHANNEL_NAME', help="Export the given Public Channels")


    args = parser.parse_args()
    slack = Slacker(args.token)
    dump_id_files = args.dump_id_files
    zip_name = args.zip
    users, channels = bootstrap_key_values(slack, dry_run)

    if not args.data_dir:
        output_directory = "outputs/{0}-slack_export".format(datetime.today().strftime("%Y%m%d-%H%M%S"))
        abs_output_directory = os.path.join(root_path, 'outputs')
        abs_slack_export_directory = os.path.join(root_path, output_directory)

        mkdir(output_directory)
        os.chdir(output_directory)

        if dump_id_files:
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

    else:
        output_directory = "outputs/{}".format(args.data_dir)
        abs_output_directory = os.path.join(root_path, 'outputs')
        abs_slack_export_directory = os.path.join(root_path, output_directory)
        os.chdir(output_directory)


    # ---------------------------- Parameters for BigQuery ---------------------------- #
    phase = args.gbq_phase
    project_id = bigquery_config[phase]['project']            # geultto
    if phase == 'production':
        prod_table_suffix = bigquery_config[phase]['suffix']           # prod
        prod_log_table_id = 'slack_log.3rd_{}'.format(prod_table_suffix)
        prod_status_table_id = 'status_board.3rd_{}'.format(prod_table_suffix)
    else:
        prod_table_suffix = None
        prod_log_table_id = None
        prod_status_table_id = None
    table_suffix = bigquery_config['development']['suffix']           # staging
    log_table_id = 'slack_log.3rd_{}'.format(table_suffix)
    status_table_id = 'status_board.3rd_{}'.format(table_suffix)


    print('Getting data from BigQuery...')
    all_deadline_dates = get_deadline_data(abs_output_directory)
    # submit_num
    deadline_time = datetime.strptime(args.deadline, '%Y-%m-%d')
    submit_num = int(all_deadline_dates.loc[all_deadline_dates['date'] == deadline_time].index[0])
    # feedback check는 submit_num이 1 이상일 때부터 하므로
    # submit_num == 0일 때는 전체 데이터를 수집하지 않음
    if submit_num == 0:
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
    all_channels = os.listdir(abs_slack_export_directory)
    filtered_channels = sorted([i for i in all_channels if i.startswith(args.channel_prefix)])
    # collect all data from filtered channels
    all_messages = get_all_messages(filtered_channels, abs_slack_export_directory)

    print('Done.\n')


    # ---------------------------- submit / pass / feedback check ---------------------------- #
    # ------------------- save data as pandas DataFrame & send to BigQuery ------------------- #
    run_all = args.run_all
    if run_all:
        print('Check and build with all deadline dates.\n')
        print('Checking all messages by reactions...')
        # 지금 파라미터로 들어온 deadline 이전 날짜에 대해 모두 돌리기
        for num in range(submit_num+1):
            deadline = all_deadline_dates.loc[num]['date'].strftime('%Y-%m-%d')
            dataz = all_message_check(
                users,
                deadline,
                all_deadline_dates,
                peer_reviewers,
                num,
                all_slack_log,
                all_status_board,
                all_messages)
            print('{}/{} messages checked. Sending Data to BigQuery...'.format(num+1, submit_num+1))

            # 첫 번째 날짜는 replace로, 두 번째부터는 append로 돌리기
            if num == 0:
                if_exists_prod = 'replace'
            else:
                if_exists_prod = 'append'
            send_data_to_gbq(dataz, phase, project_id, log_table_id, status_table_id, \
                             prod_log_table_id, prod_status_table_id, if_exists_prod)
            print('Sent.'.format(num+1, submit_num))
            time.sleep(10)
        print('\nSuccesfully All sent.')

    # run only this submission
    else:
        print('Check and build data by only this submission. \nAppending data at the end of table made before.\n')
        print('Checking all messages by reactions...')
        dataz = all_message_check(
            users,
            args.deadline,
            all_deadline_dates,
            peer_reviewers,
            submit_num,
            all_slack_log,
            all_status_board,
            all_messages)

        print('All messages checked.\n\nSending Data to BigQuery...')
        if submit_num > 0:
            if_exists_prod = 'append'
        else:
            if_exists_prod = 'replace'
        send_data_to_gbq(dataz, phase, project_id, log_table_id, status_table_id, \
                         prod_log_table_id, prod_status_table_id, if_exists_prod)

        print('Succesfully All sent.')
