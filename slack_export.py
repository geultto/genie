from slacker import Slacker
import json
import argparse
import os
import io
import shutil
import copy
from datetime import datetime
from pick import pick
from time import sleep

# fetches the complete message history for a channel/group/im
#
# pageableObject could be:
# slack.channel
# slack.groups
# slack.im
#

userNamesById = {}
userIdsByName = {}
slack = None
dryRun = None

# channelId is the id of the channel/group/im you want to download history for.
def getHistory(pageableObject, channelId, pageSize = 100):
    messages = []
    lastTimestamp = None

    while(True):
        response = pageableObject.history(
            channel = channelId,
            latest    = lastTimestamp,
            oldest    = 0,
            count     = pageSize
        ).body

        messages.extend(response['messages'])

        if (response['has_more'] == True):
            lastTimestamp = messages[-1]['ts'] # -1 means last element in a list
            sleep(1) # Respect the Slack API rate limit
        else:
            break
    return messages


def mkdir(directory):
    if not os.path.isdir(directory):
        os.makedirs(directory)


# create datetime object from slack timestamp ('ts') string
def parseTimeStamp( timeStamp ):
    if '.' in timeStamp:
        t_list = timeStamp.split('.')
        if len( t_list ) != 2:
            raise ValueError( 'Invalid time stamp' )
        else:
            return datetime.utcfromtimestamp( float(t_list[0]) )


# move channel files from old directory to one with new channel name
def channelRename( oldRoomName, newRoomName ):
    # check if any files need to be moved
    if not os.path.isdir( oldRoomName ):
        return
    mkdir( newRoomName )
    for fileName in os.listdir( oldRoomName ):
        shutil.move( os.path.join( oldRoomName, fileName ), newRoomName )
    os.rmdir( oldRoomName )


def writeMessageFile( fileName, messages ):
    directory = os.path.dirname(fileName)

    if not os.path.isdir( directory ):
        mkdir( directory )

    with open(fileName, 'w') as outFile:
        json.dump( messages, outFile, indent=4)


# parse messages by date
def parseMessages( roomDir, messages, roomType ):
    nameChangeFlag = roomType + "_name"

    currentFileDate = ''
    currentMessages = []
    for message in messages:
        #first store the date of the next message
        ts = parseTimeStamp( message['ts'] )
        fileDate = '{:%Y-%m-%d}'.format(ts)

        #if it's on a different day, write out the previous day's messages
        if fileDate != currentFileDate:
            outFileName = '{room}/{file}.json'.format( room = roomDir, file = currentFileDate )
            writeMessageFile( outFileName, currentMessages )
            currentFileDate = fileDate
            currentMessages = []

        # check if current message is a name change
        # dms won't have name change events
        if roomType != "im" and ( 'subtype' in message ) and message['subtype'] == nameChangeFlag:
            roomDir = message['name']
            oldRoomPath = message['old_name']
            newRoomPath = roomDir
            channelRename( oldRoomPath, newRoomPath )

        currentMessages.append( message )
    outFileName = '{room}/{file}.json'.format( room = roomDir, file = currentFileDate )
    writeMessageFile( outFileName, currentMessages )

def filterConversationsByName(channelsOrGroups, channelOrGroupNames):
    return [conversation for conversation in channelsOrGroups if conversation['name'] in channelOrGroupNames]

def promptForPublicChannels(channels):
    channelNames = [channel['name'] for channel in channels]
    selectedChannels = pick(channelNames, 'Select the Public Channels you want to export:', multi_select=True)
    return [channels[index] for channelName, index in selectedChannels]

# fetch and write history for all public channels
def fetchPublicChannels(channels, channel_prefix):
    global dryRun, slack
    if dryRun:
        print("Public Channels selected for export:")
        for channel in channels:
            print(channel['name'])
        print()
        return

    for channel in channels:
        channelDir = channel['name']
        if channelDir.startswith(channel_prefix):
            print("Fetching history for Public Channel: {0}".format(channelDir))
            channelDir = channel['name']
            mkdir( channelDir )
            messages = getHistory(slack.channels, channel['id'])
            parseMessages( channelDir, messages, 'channel')

# write channels.json file
def dumpChannelFile(channels):
    print("\nMaking channels file")

    #We will be overwriting this file on each run.
    with open('channels.json', 'w') as outFile:
        json.dump( channels , outFile, indent=4)

def filterDirectMessagesByUserNameOrId(dms, userNamesOrIds):
    global userIdsByName
    userIds = [userIdsByName.get(userNameOrId, userNameOrId) for userNameOrId in userNamesOrIds]
    return [dm for dm in dms if dm['user'] in userIds]

def promptForDirectMessages(dms):
    global userNamesById
    dmNames = [userNamesById.get(dm['user'], dm['user'] + " (name unknown)") for dm in dms]
    selectedDms = pick(dmNames, 'Select the 1:1 DMs you want to export:', multi_select=True)
    return [dms[index] for dmName, index in selectedDms]

# fetch and write history for all direct message conversations
# also known as IMs in the slack API.
def fetchDirectMessages(dms):
    global userNamesById, dryRun, slack
    if dryRun:
        print("1:1 DMs selected for export:")
        for dm in dms:
            print(userNamesById.get(dm['user'], dm['user'] + " (name unknown)"))
        print()
        return

    for dm in dms:
        name = userNamesById.get(dm['user'], dm['user'] + " (name unknown)")
        print("Fetching 1:1 DMs with {0}".format(name))
        dmId = dm['id']
        mkdir(dmId)
        messages = getHistory(slack.im, dm['id'])
        parseMessages( dmId, messages, "im" )

def promptForGroups(groups):
    groupNames = [group['name'] for group in groups]
    selectedGroups = pick(groupNames, 'Select the Private Channels and Group DMs you want to export:', multi_select=True)
    return [groups[index] for groupName, index in selectedGroups]

# fetch and write history for specific private channel
# also known as groups in the slack API.
def fetchGroups(groups):
    global dryRun, slack
    if dryRun:
        print("Private Channels and Group DMs selected for export:")
        for group in groups:
            print(group['name'])
        print()
        return

    for group in groups:
        groupDir = group['name']
        mkdir(groupDir)
        messages = []
        print("Fetching history for Private Channel / Group DM: {0}".format(group['name']))
        messages = getHistory(slack.groups, group['id'])
        parseMessages( groupDir, messages, 'group' )

# fetch all users for the channel and return a map userId -> userName
def getUserMap(users):
    global userNamesById, userIdsByName

    for user in users:
        userNamesById[user['id']] = user['name']
        userIdsByName[user['name']] = user['id']

# stores json of user info
def dumpUserFile(users):
    #write to user file, any existing file needs to be overwritten.
    with open( "users.json", 'w') as userFile:
        json.dump( users, userFile, indent=4 )

# get basic info about the slack channel to ensure the authentication token works
def doTestAuth(_slack):
    global slack
    slack = _slack
    testAuth = slack.auth.test().body
    teamName = testAuth['team']
    currentUser = testAuth['user']
    print("\nSuccessfully authenticated for team {0} and user {1} ".format(teamName, currentUser))
    return testAuth

# Since Slacker does not Cache.. populate some reused lists
def bootstrapKeyValues(_dryRun):
    global slack, dryRun
    dryRun = _dryRun
    users = slack.users.list().body['members']
    print("Found {0} Users".format(len(users)))
    sleep(1)

    channels = slack.channels.list().body['channels']
    print("Found {0} Public Channels".format(len(channels)))
    sleep(1)

    getUserMap(users)

    return users, channels

# Returns the conversations to download based on the command-line arguments
def selectConversations(allConversations, commandLineArg, filter, prompt, args):
    if isinstance(commandLineArg, list) and len(commandLineArg) > 0:
        return filter(allConversations, commandLineArg)
    elif commandLineArg != None or not anyConversationsSpecified(args):
        if args.prompt:
            return prompt(allConversations)
        else:
            return allConversations
    else:
        return []

# Returns true if any conversations were specified on the command line
def anyConversationsSpecified(args):
    return args.publicChannels != None or args.groups != None or args.directMessages != None

# This method is used in order to create a empty Channel if you do not export public channels
# otherwise, the viewer will error and not show the root screen. Rather than forking the editor, I work with it.
def dumpDummyChannel():
    channelName = channels[0]['name']
    mkdir( channelName )
    fileDate = '{:%Y-%m-%d}'.format(datetime.today())
    outFileName = '{room}/{file}.json'.format(room=channelName, file=fileDate )
    writeMessageFile(outFileName, [])

def finalize(zipName):
    os.chdir('..')
    if zipName:
        shutil.make_archive(zipName, 'zip', output_directory, None)
        shutil.rmtree(output_directory)
