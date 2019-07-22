from slack_export import *
import os, json, csv
import pandas as pd
import yaml


if __name__ == "__main__":

    ## ---------------------------- save raw data from slack ---------------------------- ##
    # It will generate data files in output Directory
    slack_token = os.getenv('SLACK_TOKEN')

    parser = argparse.ArgumentParser(description='Export Slack history')

    parser.add_argument('--token', default=slack_token, help="Slack API token")
    parser.add_argument('--zip', help="Name of a zip file to outputs as")
    parser.add_argument('--channel_prefix', default=None, required=True, help="prefix of channel which need to be exported")

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

    users, channels, groups, dms = bootstrapKeyValues(dryRun)

    outputDirectory = "{0}-slack_export".format(datetime.today().strftime("%Y%m%d-%H%M%S"))

    mkdir(outputDirectory)
    os.chdir(outputDirectory)

    if not dryRun:
        dumpUserFile(users)
        dumpChannelFile(groups, dms, channels, tokenOwnerId)

    selectedChannels = selectConversations(
        channels,
        args.publicChannels,
        filterConversationsByName,
        promptForPublicChannels,
        args)

    if len(selectedChannels) > 0:
        fetchPublicChannels(selectedChannels, args.channel_prefix)

    print('\noutputs Directory: %s' % outputDirectory)

    finalize(zipName)


    ## ---------------------------- Get users url data by reactions ---------------------------- ##
    # Get user data
    users = {}
    with open(os.path.join(outputDirectory, 'users.json')) as json_file:
        user_json = json.load(json_file)
        for user in user_json:
            if not (user['is_bot'] or (user['name'] == 'slackbot')):
                if 'real_name' in user.keys():
                    users[user['id']] = user['real_name']
                else:
                    users[user['id']] = user['profile']['real_name']

    # Get Filtered URLs from saved data
    # need dataz only from [3_*] channels
    all_channels = os.listdir(outputDirectory)
    filtered_channels = sorted([i for i in all_channels if i.startswith(args.channel_prefix)])

    # collect all data from filtered channels
    all_messages = []
    for channel in filtered_channels:
        channel_path = os.path.join(outputDirectory, channel)
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
                else:
                    return False
            else:
                return False

    # filter data with reactions
    submit_reaction = 'submit'
    pass_reaction = 'pass'
    pass_users = []
    submit_urlz = []
    for message in all_messages:
        # 1) submit
        if ('attachments' in message.keys()) and ('reactions' in message.keys()):
            if self_reaction_check(submit_reaction, message):
                userId = message['user']
                link = message['attachments'][0]['title_link']
                time = str(datetime.fromtimestamp(float(message['ts'])))[:-7]

                url_data = {'userId': userId, 'url': link, 'time': time}
                submit_urlz.append(url_data)

        # 2) pass
        if ('reactions' in message.keys()) and (self_reaction_check(pass_reaction, message)):
            userId = message['user']
            time = str(datetime.fromtimestamp(float(message['ts'])))[:-7]
            pass_user_data = {'userId': userId, 'time': time}
            pass_users.append(pass_user_data)
