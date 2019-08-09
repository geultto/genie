import argparse
import re
from slacker import Slacker
import pandas as pd
from slack_export import *
from utils import bigquery_config, root_path
from checker import self_reaction_check, check_deadline
from extract_data import get_status_data, get_deadline_data

if __name__ == "__main__":
    # ---------------------------- save raw data from slack ---------------------------- #
    # It will generate data files in output Directory
    slack_token = os.getenv('SLACK_TOKEN')
    parser = argparse.ArgumentParser(description='Export Slack history')

    parser.add_argument('--token', default=slack_token, help="Slack API token")
    parser.add_argument('--zip', help="Name of a zip file to outputs as")
    parser.add_argument('--channel_prefix', default='3_', help="prefix of channel which need to be exported")
    parser.add_argument('--gbq_phase', default='development', help='BigQuery dealing phase: development / production')
    parser.add_argument('--deadline', default='2019-07-22', help='deadline date (sunday): year-month-date')

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
    table_suffix = bigquery_config[phase]['suffix']           # prod / staging
    log_table_id = 'slack_log.3rd_{}'.format(table_suffix)
    status_table_id = 'status_board.3rd_{}'.format(table_suffix)

    all_deadline_dates = get_deadline_data()

    # ---------------------------- Get users url data by reactions ---------------------------- #
    # Get user data
    users = {}
    abs_output_directory = os.path.join(root_path, output_directory)
    with open(os.path.join(abs_output_directory, 'users.json')) as json_file:
        user_json = json.load(json_file)
        for user in user_json:
            if not (user['is_bot'] or (user['name'] == 'slackbot')):
                if 'real_name' in user.keys():
                    users[user['id']] = user['real_name']
                else:
                    users[user['id']] = user['profile']['real_name']

    # Get Filtered URLs from saved data
    # need dataz only from [3_*] channels
    all_channels = os.listdir(abs_output_directory)
    filtered_channels = sorted([i for i in all_channels if i.startswith(args.channel_prefix)])

    # collect all data from filtered channels
    all_messages = []
    for channel in filtered_channels:
        channel_path = os.path.join(abs_output_directory, channel)
        for date in sorted(os.listdir(channel_path)):
            with open(os.path.join(channel_path, date)) as json_file:
                json_data = json.load(json_file)
                all_messages.extend(json_data)

    # filter data with reactions
    submit_reaction = 'submit'
    pass_reaction = 'pass'
    submit_data = pd.DataFrame({'user_id': userid, 'url': -1, 'time': -1, 'deadline_check': None, 'message_id': None} \
                               for userid in users)
    for message in all_messages:
        message_id = message['client_msg_id']
        # 1) submit
        if (('attachments' in message.keys()) or ('<https://' in message['text'])) and ('reactions' in message.keys()):
            if self_reaction_check(submit_reaction, message):
                user_id = message['user']
                if 'attachments' in message.keys():
                    link = message['attachments'][0]['title_link']
                # naver blog의 경우 attachments로 생성 안되어 regex로 잡기
                elif '<https://' in message['text']:
                    message_text = message['text']
                    pattern = re.compile('<https://.+?>')
                    link = pattern.search(message_text)
                    link = link.group()[1:-1]
                # 이 부분 에러날 수 있겠네요! if문, elif 처리 후, else쪽에 없어서 밑에 link가 할당되지 않는 경우가 있을 수 있음

                time = str(datetime.fromtimestamp(float(message['ts'])))[:-7]
                is_in_deadline = check_deadline(args.deadline, time, submit_reaction)

                if is_in_deadline:
                    submit_data.loc[submit_data['user_id'] == user_id, 'url'] = link
                else:
                    submit_data.loc[submit_data['user_id'] == user_id, 'url'] = -1
                submit_data.loc[submit_data['user_id'] == user_id, 'time'] = time
                submit_data.loc[submit_data['user_id'] == user_id, 'deadline_check'] = is_in_deadline
                submit_data.loc[submit_data['user_id'] == user_id, 'message_id'] = message_id

        # 2) pass
        if ('reactions' in message.keys()) and (self_reaction_check(pass_reaction, message)):
            user_id = message['user']
            time = str(datetime.fromtimestamp(float(message['ts'])))[:-7]
            is_in_deadline = check_deadline(args.deadline, time, pass_reaction)

            if is_in_deadline:
                submit_data.loc[submit_data['user_id'] == user_id, 'url'] = 'pass'
            else:
                submit_data.loc[submit_data['user_id'] == user_id, 'url'] = -1
            submit_data.loc[submit_data['user_id'] == user_id, 'time'] = time
            submit_data.loc[submit_data['user_id'] == user_id, 'deadline_check'] = is_in_deadline
            submit_data.loc[submit_data['user_id'] == user_id, 'message_id'] = message_id

    with open(os.path.join(root_path, 'outputs/all_messages.json'), 'w') as out_file:
        json.dump(all_messages, out_file, indent=4)

    # -------------------- save data as pandas DataFrame & send to BigQuery -------------------- #
    print('Sending Data to BigQuery...')

    submit_data = submit_data[['user_id', 'message_id', 'time', 'deadline_check', 'url']]
    submit_data.to_gbq(log_table_id, project_id=project_id, if_exists='replace')

    status_df = get_status_data()
    status_df.to_gbq(status_table_id, project_id=project_id, if_exists='replace')

    print('Succesfully sended.')
