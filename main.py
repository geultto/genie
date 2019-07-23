from slack_export import *
import os, json, csv, re
import pandas as pd
from utils import bigquery_config, root_path
from datetime import timedelta


if __name__ == "__main__":

    ## ---------------------------- save raw data from slack ---------------------------- ##
    # It will generate data files in output Directory
    slack_token = os.getenv('SLACK_TOKEN')

    parser = argparse.ArgumentParser(description='Export Slack history')

    parser.add_argument('--token', default=slack_token, help="Slack API token")
    parser.add_argument('--zip', help="Name of a zip file to outputs as")
    parser.add_argument('--channel_prefix', default=None, required=True, help="prefix of channel which need to be exported")
    parser.add_argument('--gbq_phase', default=None, required=True, help='BigQuery dealing phase: development / production')
    parser.add_argument('--deadline', default=None, required=True, help='deadline date (sunday): year-month-date')

    parser.add_argument(
        '--dryRun',
        action='store_true',
        default=False,
        help="List the conversations that will be exported (don't fetch/write history)")

    parser.add_argument(
        '--publicChannels',
        nargs='*',
        default=None,
        metavar='CHANNEL_NAME',
        help="Export the given Public Channels")

    parser.add_argument(
        '--groups',
        nargs='*',
        default=None,
        metavar='GROUP_NAME',
        help="Export the given Private Channels / Group DMs")

    parser.add_argument(
        '--directMessages',
        nargs='*',
        default=None,
        metavar='USER_NAME',
        help="Export 1:1 DMs with the given users")

    parser.add_argument(
        '--prompt',
        action='store_true',
        default=False,
        help="Prompt you to select the conversations to export")

    args = parser.parse_args()

    slack = Slacker(args.token)

    testAuth = doTestAuth(slack)
    tokenOwnerId = testAuth['user_id']

    dryRun = args.dryRun
    zipName = args.zip

    users, channels = bootstrapKeyValues(dryRun)

    outputDirectory = "outputs/{0}-slack_export".format(datetime.today().strftime("%Y%m%d-%H%M%S"))

    mkdir(outputDirectory)
    os.chdir(outputDirectory)

    if not dryRun:
        dumpUserFile(users)
        dumpChannelFile(channels)

    selectedChannels = selectConversations(
        channels,
        args.publicChannels,
        filterConversationsByName,
        promptForPublicChannels,
        args)

    if len(selectedChannels) > 0:
        fetchPublicChannels(selectedChannels, args.channel_prefix)

    print('\nAll message data saved.\noutputs Directory: [%s]\n' % outputDirectory)

    finalize(zipName)


    ## ---------------------------- Parameters for BigQuery ---------------------------- ##
    phase = args.gbq_phase
    project_id = bigquery_config[phase]['project']            # geultto
    table_suffix = bigquery_config[phase]['suffix']           # prod / staging
    log_table_id = 'slack_log.3rd_{}'.format(table_suffix)
    status_table_id = 'status_board.3rd_{}'.format(table_suffix)

    # feedback 위해 글또 3기의 제출 마감 날짜 목록 추출
    query_get_dates = '''
    select date
    from `geultto.peer_reviewer.3rd_prod`
    group by date
    '''
    all_deadline_dates = pd.read_gbq(query_get_dates, project_id=project_id, dialect='standard')


    ## ---------------------------- Get users url data by reactions ---------------------------- ##
    # Get user data
    users = {}
    abs_outputDirectory = os.path.join(root_path, outputDirectory)
    with open(os.path.join(abs_outputDirectory, 'users.json')) as json_file:
        user_json = json.load(json_file)
        for user in user_json:
            if not (user['is_bot'] or (user['name'] == 'slackbot')):
                if 'real_name' in user.keys():
                    users[user['id']] = user['real_name']
                else:
                    users[user['id']] = user['profile']['real_name']

    # Get Filtered URLs from saved data
    # need dataz only from [3_*] channels
    all_channels = os.listdir(abs_outputDirectory)
    filtered_channels = sorted([i for i in all_channels if i.startswith(args.channel_prefix)])

    # collect all data from filtered channels
    all_messages = []
    for channel in filtered_channels:
        channel_path = os.path.join(abs_outputDirectory, channel)
        for date in sorted(os.listdir(channel_path)):
            with open(os.path.join(channel_path, date)) as json_file:
                json_data = json.load(json_file)
                all_messages.extend(json_data)

    def self_reaction_check(check_reaction, message):
        '''
        메세지에 self로 check_reaction을 입력했는지 확인
        '''
        reactions = message['reactions']
        userId = message['user']
        for reaction in reactions:
            if reaction['name'] == check_reaction:
                if userId in reaction['users']:
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


    # filter data with reactions
    submit_reaction = 'submit'
    pass_reaction = 'pass'
    submit_data = pd.DataFrame({'userId': userid, 'url': -1, 'time': -1, 'deadline_check': None, 'message_id': None} \
                               for userid in users)
    for message in all_messages:
        # 1) submit
        if (('attachments' in message.keys()) or ('<https://' in message['text'])) and ('reactions' in message.keys()):
            if self_reaction_check(submit_reaction, message):
                userId = message['user']
                if 'attachments' in message.keys():
                    link = message['attachments'][0]['title_link']
                # naver blog의 경우 attachments로 생성 안되어 regex로 잡기
                elif ('<https://' in message['text']):
                    message_text = message['text']
                    pattern = re.compile('<https://.+?>')
                    link = pattern.search(message_text)
                    link = link.group()[1:-1]

                message_id = message['client_msg_id']
                time = str(datetime.fromtimestamp(float(message['ts'])))[:-7]
                isindeadline = check_deadline(args.deadline, time, submit_reaction)

                if isindeadline:
                    submit_data.loc[submit_data['userId'] == userId, 'url'] = link
                else:
                    submit_data.loc[submit_data['userId'] == userId, 'url'] = -1
                submit_data.loc[submit_data['userId'] == userId, 'time'] = time
                submit_data.loc[submit_data['userId'] == userId, 'deadline_check'] = isindeadline
                submit_data.loc[submit_data['userId'] == userId, 'message_id'] = message_id

        # 2) pass
        if ('reactions' in message.keys()) and (self_reaction_check(pass_reaction, message)):
            userId = message['user']
            time = str(datetime.fromtimestamp(float(message['ts'])))[:-7]
            isindeadline = check_deadline(args.deadline, time, pass_reaction)

            if isindeadline:
                submit_data.loc[submit_data['userId'] == userId, 'url'] = 'pass'
            else:
                submit_data.loc[submit_data['userId'] == userId, 'url'] = -1
            submit_data.loc[submit_data['userId'] == userId, 'time'] = time
            submit_data.loc[submit_data['userId'] == userId, 'deadline_check'] = isindeadline
            submit_data.loc[submit_data['userId'] == userId, 'message_id'] = message_id

    with open(os.path.join(root_path, 'outputs/all_messages.json'), 'w') as outFile:
        json.dump( all_messages , outFile, indent=4)

    ## -------------------- save data as pandas DataFrame & send to BigQuery -------------------- ##
    print('Sending Data to BigQuery...')

    submit_data.to_gbq(log_table_id, project_id=project_id, if_exists='replace')

    # status_table에 원하는 데이터만 추출해서 생성
    query_status_table = '''
    select name, team, time, url
    from `geultto.slack_log.3rd_staging` as l left outer join `geultto.user_db.team_member` as r
    on l.userId = r.id
    where name IS NOT NULL
    '''
    status_table = pd.read_gbq(query_status_table, project_id=project_id, dialect='standard')
    status_table.to_gbq(status_table_id, project_id=project_id, if_exists='replace')

    print('Succesfully sended.')
