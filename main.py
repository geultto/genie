from slack_export import *
import os

if __name__ == "__main__":

    slack_token = os.getenv('SLACK_TOKEN')

    parser = argparse.ArgumentParser(description='Export Slack history')

    parser.add_argument('--token', default=slack_token, help="Slack API token")
    parser.add_argument('--zip', help="Name of a zip file to outputs as")

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

    selectedGroups = selectConversations(
        groups,
        args.groups,
        filterConversationsByName,
        promptForGroups,
        args)

    selectedDms = selectConversations(
        dms,
        args.directMessages,
        filterDirectMessagesByUserNameOrId,
        promptForDirectMessages,
        args)

    if len(selectedChannels) > 0:
        fetchPublicChannels(selectedChannels)

    if len(selectedGroups) > 0:
        if len(selectedChannels) == 0:
            dumpDummyChannel()
        fetchGroups(selectedGroups)

    if len(selectedDms) > 0:
        fetchDirectMessages(selectedDms)

    print('\noutputs Directory: %s' % outputDirectory)

    finalize(zipName)
