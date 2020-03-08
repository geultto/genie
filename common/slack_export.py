import json
import os
import shutil
from datetime import datetime
from pick import pick
from time import sleep


user_names_by_id = {}
user_ids_by_name = {}
dry_run = None


def get_history(pageable_object, channel_id, page_size=100):
    messages = []
    last_timestamp = None

    while True:
        response = pageable_object.history(
            channel=channel_id,
            latest=last_timestamp,
            oldest=0,
            count=page_size
        ).body

        messages.extend(response['messages'])

        if response['has_more']:
            last_timestamp = messages[-1]['ts']  # -1 means last element in a list
            sleep(1)  # Respect the Slack API rate limit
        else:
            break
    return messages


def mkdir(directory):
    if not os.path.isdir(directory):
        os.makedirs(directory)


def parse_time_stamp(time_stamp):
    """
    create datetime object from slack timestamp ('ts') string
    """
    if '.' in time_stamp:
        t_list = time_stamp.split('.')
        if len(t_list) != 2:
            raise ValueError('Invalid time stamp')
        else:
            return datetime.utcfromtimestamp(float(t_list[0]))


def channel_rename(old_room_name, new_room_name):
    """
    move channel files from old directory to one with new channel name
    """
    # check if any files need to be moved
    if not os.path.isdir(old_room_name):
        return
    mkdir(new_room_name)
    for file_name in os.listdir(old_room_name):
        shutil.move(os.path.join(old_room_name, file_name), new_room_name)
    os.rmdir(old_room_name)


def write_message_file(file_name, messages):
    directory = os.path.dirname(file_name)

    if not os.path.isdir(directory):
        mkdir(directory)

    with open(file_name, 'w') as out_file:
        json.dump(messages, out_file, indent=4)


def parse_messages(room_dir, messages, room_type):
    """
    parse messages by date
    """
    name_change_flag = room_type + "_name"

    current_file_date = ''
    current_messages = []
    for message in messages:
        # first store the date of the next message
        ts = parse_time_stamp(message['ts'])
        file_date = '{:%Y-%m-%d}'.format(ts)

        # if it's on a different day, write out the previous day's messages
        if file_date != current_file_date:
            out_file_name = '{room}/{file}.json'.format(room=room_dir, file=current_file_date)
            write_message_file(out_file_name, current_messages)
            current_file_date = file_date
            current_messages = []

        # check if current message is a name change
        # dms won't have name change events
        if room_type != "im" and ('subtype' in message) and message['subtype'] == name_change_flag:
            room_dir = message['name']
            old_room_path = message['old_name']
            new_room_path = room_dir
            channel_rename(old_room_path, new_room_path)

        current_messages.append(message)
    out_file_name = '{room}/{file}.json'.format(room=room_dir, file=current_file_date)
    write_message_file(out_file_name, current_messages)


def filter_conversations_by_name(channels_or_groups, channel_or_group_names):
    return [conversation for conversation in channels_or_groups if conversation['name'] in channel_or_group_names]


def prompt_for_public_channels(channels):
    channel_names = [channel['name'] for channel in channels]
    selected_channels = pick(channel_names, 'Select the Public Channels you want to export:', multi_select=True)
    return [channels[index] for channelName, index in selected_channels]


def fetch_public_channels(channels, channel_prefix):
    """
    fetch and write history for all public channels
    """
    global dry_run
    if dry_run:
        print("Public Channels selected for export:")
        for channel in channels:
            print(channel['name'])
        print()
        return

    for channel in channels:
        channel_dir = channel['name']
        if channel_dir.startswith(channel_prefix):
            print("Fetching history for Public Channel: {0}".format(channel_dir))
            channel_dir = channel['name']
            mkdir(channel_dir)
            messages = get_history(slack.channels, channel['id'])
            parse_messages(channel_dir, messages, 'channel')


def dump_channel_file(channels):
    """
    write channels.json file
    """
    print("\nMaking channels file")

    # We will be overwriting this file on each run.
    with open('../channels.json', 'w') as out_file:
        json.dump(channels, out_file, indent=4)


def get_user_map(users):
    """
    fetch all users for the channel and return a map user_id -> user_name
    """
    global user_names_by_id, user_ids_by_name

    for user in users:
        user_names_by_id[user['id']] = user['name']
        user_ids_by_name[user['name']] = user['id']


def dump_user_file(users):
    """
    stores json of user info
    write to user file, any existing file needs to be overwritten.
    """
    with open(os.path.join('../', 'users.json'), 'w') as userFile:
        json.dump(users, userFile, indent=4)


def bootstrap_key_values(slack_obj, _dry_run):
    """
    Since Slacker does not Cache.. populate some reused lists
    """
    global slack
    slack = slack_obj
    dry_run = _dry_run
    users = slack.users.list().body['members']
    print("Found {0} Users".format(len(users)))
    sleep(1)

    channels = slack.channels.list().body['channels']
    print("Found {0} Public Channels".format(len(channels)))
    sleep(1)

    get_user_map(users)

    return users, channels


def select_conversations(all_conversations, commandline_arg, filter, prompt, args):
    """
    Returns the conversations to download based on the command-line arguments
    """
    if isinstance(commandline_arg, list) and len(commandline_arg) > 0:
        return filter(all_conversations, commandline_arg)
    elif commandline_arg is not None or not any_conversations_specified(args):
        if args.prompt:
            return prompt(all_conversations)
        else:
            return all_conversations
    else:
        return []


def any_conversations_specified(args):
    """
    Returns true if any conversations were specified on the command line
    """
    return args.public_channels is not None


def finalize(zip_name, output_directory):
    os.chdir('..')
    if zip_name:
        shutil.make_archive(zip_name, 'zip', output_directory, None)
        shutil.rmtree(output_directory)


def get_user_names_with_channel(channels, channel_prefix):
    users_with_channel = []

    for channel in channels:
        channel_name = channel['name']

        if channel_name.startswith(channel_prefix):
            member_names = channel['topic']['value'].split()

            for name in member_names:
                users_with_channel.append({'name': name, 'channel_name': channel_name})

    return users_with_channel


def get_user_names_with_id(users):
    users_with_id = []

    for user in users:
        if is_valid_user(user):
            users_with_id.append({'name': user['real_name'], 'id': user['id']})

    return users_with_id


def is_valid_user(user):
    return not user['is_bot'] and not user['deleted'] and user['real_name'] != 'Slackbot'
