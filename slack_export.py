import json
import argparse
import os
import io
import shutil
import copy
import requests
import sys
from datetime import datetime
from pick import pick
from time import sleep
import functools

from slack import WebClient
from slack.errors import SlackApiError

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

    # if there's no data to write to the file, return
    if not messages:
        return

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
            outFileName = u'{room}/{file}.json'.format( room = roomDir, file = currentFileDate )
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
    outFileName = u'{room}/{file}.json'.format( room = roomDir, file = currentFileDate )
    writeMessageFile( outFileName, currentMessages )

def filterConversationsByName(channelsOrGroups, channelOrGroupNames):
    return [conversation for conversation in channelsOrGroups if conversation['name'] in channelOrGroupNames]

def promptForPublicChannels(channels):
    channelNames = [channel['name'] for channel in channels]
    selectedChannels = pick(channelNames, 'Select the Public Channels you want to export:', multi_select=True)
    return [channels[index] for channelName, index in selectedChannels]

# fetch and write history for all public channels
def fetchPublicChannels(channels):
    if dryRun:
        print("Public Channels selected for export:")
        for channel in channels:
            print(channel['name'])
        print()
        return

    for channel in channels:
        channelDir = channel['name']
        print("Fetching history for Public Channel: {0}".format(channelDir))
        mkdir( channelDir )
        messages = getEntireChannelHistory(channel['id'])
        parseMessages( channelDir, messages, 'channel')

# write channels.json file
def dumpChannelFile():
    print("Making channels file")

    private = []
    mpim = []

    for group in groups:
        if group['is_mpim']:
            mpim.append(group)
            continue
        private.append(group)
    
    # slack viewer wants DMs to have a members list, not sure why but doing as they expect
    for dm in dms:
        dm['members'] = [dm['user'], tokenOwnerId]

    #We will be overwriting this file on each run.
    with open('channels.json', 'w') as outFile:
        json.dump( channels , outFile, indent=4)
    with open('groups.json', 'w') as outFile:
        json.dump( private , outFile, indent=4)
    with open('mpims.json', 'w') as outFile:
        json.dump( mpim , outFile, indent=4)
    with open('dms.json', 'w') as outFile:
        json.dump( dms , outFile, indent=4)

def filterDirectMessagesByUserNameOrId(dms, userNamesOrIds):
    userIds = [userIdsByName.get(userNameOrId, userNameOrId) for userNameOrId in userNamesOrIds]
    return [dm for dm in dms if dm['user'] in userIds]

def promptForDirectMessages(dms):
    dmNames = [userNamesById.get(dm['user'], dm['user'] + " (name unknown)") for dm in dms]
    selectedDms = pick(dmNames, 'Select the 1:1 DMs you want to export:', multi_select=True)
    return [dms[index] for dmName, index in selectedDms]

def guessListDataKey(obj, oldkey):
    if oldkey:
        return oldkey
    #key messages, channels, members, thing*s* etc.
    for k in obj:
        if isinstance(k, str) and k.endswith('s') and isinstance(obj[k], list):
            return k

#Gather all the paged data from a SlackResponse
def get(func, datakey=None):
    arr = []
    try:
        i = 0
        for page in func(limit=1000):
            #TODO don't think you can hit this page['ok'] false throws exception.
            i += 1
            if page['ok']:
                datakey = guessListDataKey(page.data, datakey)
                arr.extend(page[datakey])
                sys.stdout.write('.')
                if i % 40 == 0:
                    print("")
                else:
                    sys.stdout.flush()

                #Some functions don't have 'has_more'
                #if page['has_more']:
                cursor = ''
                meta = page["response_metadata"]
                if meta:
                    cursor = meta["next_cursor"]
                sleep(1)
                if cursor == '':
                    break
    except SlackApiError as e:
        print(e)
        data = e.response.data
        if data['error'] == 'missing_scope':
            print("Your OAuth Token at http://api.slack.com/apps is missing permission:  " + data['needed'])
            print(data['needed'] + " permission can be added to the 'OAuth & Permissions' page, then click 'Reinstall App' at the top of the page.")
        exit(-1)
    print(' ' + str(len(arr)) + ' ' + datakey)
    return arr

#May need this later if guessListDataKey starts failing with api change.
#def getMessages(func):
#    return get(func, datakey="messages")
#def getMembers(func):
#    return get(func, datakey="members")
#def getChannels(func):
#    return get(func, datakey="channels")

def getThreadHistory(channel, ts):
    global client
    print("Get thread history for " + channel + " at " + ts)
    threadHistory = functools.partial(client.conversations_replies, channel=channel, ts=ts)
    return get(threadHistory)

def getChannelHistory(channel):
    global client
    print("Get history for " + channel)
    channelHistory = functools.partial(client.conversations_history, channel=channel)
    return get(channelHistory)

#This is unused by changes by mdevey to _build_threads reader.py but useful to legacy slack-export-viewer.
def addThreadSummary(msgs):
    itr = iter(msgs)
    first = next(itr) # summary is needed here / step over it.
    replies = []
    for r in itr:
        if 'user' in r:
            user = r['user']
        if 'bot_id' in r:
            user = r['bot_id']
        replies.append({"user": user, "ts": r["ts"]})
    # insert summary.
    first["replies"] = replies

def getEntireChannelHistory(channel):
    messages = []
    threadless = getChannelHistory(channel)
    #threadless.sort(key = lambda m: m['ts'])

    for msg in threadless:
         # alternatively never get the thread for a 'subtype': 'thread_broadcast'
        if "thread_ts" in msg and msg['ts'] == msg['thread_ts']:
            #toss msg and get thread instead.
            thread = getThreadHistory(channel, msg['thread_ts'])
            addThreadSummary(thread)
            messages.extend(thread)
        else:
            messages.append(msg)

    messages.sort(key = lambda m: m['ts'])

    return messages

# fetch and write history for all direct message conversations
# also known as IMs in the slack API.
def fetchDirectMessages(dms):
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
        messages = getEntireChannelHistory(dmId)
        parseMessages(dmId, messages, "im" )

def promptForGroups(groups):
    groupNames = [group['name'] for group in groups]
    selectedGroups = pick(groupNames, 'Select the Private Channels and Group DMs you want to export:', multi_select=True)
    return [groups[index] for groupName, index in selectedGroups]

# fetch and write history for specific private channel
# also known as groups in the slack API.
def fetchGroups(groups):
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
        messages = getEntireChannelHistory(group['id'])
        parseMessages( groupDir, messages, 'group' )

# fetch all users for the channel and return a map userId -> userName
def getUserMap():
    global userNamesById, userIdsByName
    for user in users:
        userNamesById[user['id']] = user['name']
        userIdsByName[user['name']] = user['id']

# stores json of user info
def dumpUserFile():
    #write to user file, any existing file needs to be overwritten.
    with open( "users.json", 'w') as userFile:
        json.dump( users, userFile, indent=4 )

# get basic info about the slack channel to ensure the authentication token works
def doTestAuth():
    global client
    try:
        auth = client.auth_test()
        print("Successfully authenticated")
        for k in auth.data:
            if not k == 'ok':
                print(k + ": " + str(auth.data[k]))
    except SlackApiError as e:
        print("AUTHENTICATION FAILED!")
        print("Create a new app at https://api.slack.com/apps")
        print("Once created visit (https://api.slack.com/apps), click your app, and double check you are pasting the correct token")
        print("Under 'Features' there is a 'OAuth & Permissions' link")
        print("This should take you to Webpage specific to your 'app' something like https://api.slack.com/apps/{ADEADBEEF}/oauth")
        print("Copy/Check the 'OAuth Access Token' and give to this script ")
        print("DO NOT SHARE THIS TOKEN WITH ANYONE (it gives access to your PRIVATE conversations)")
        print("eg\tpython3 slack_export.py --token xoxp-12341234-XXXXX-XXXXX-deadbeef")
        exit(-1)

    #TODO consider testing ALL required permissions (screwed up permission list, eg missed users:read ),
    #So we don't have to wait until get() is called late in the script.
    #All the *.permission.* calls I can find don't appear to work with a user token
    #https://api.slack.com/methods/apps.permissions.users.list I'm doing it wrong!
    #perms = client.api_call("apps.permissions.info")
    #print(perms)

    return auth

def bootstrapKeyValues():
    global users, channels, groups, dms, client
    users = get(client.users_list)
    print("Found {0} Users".format(len(users)))
    sleep(1)
    
    publicChannels = functools.partial(client.conversations_list, types=('public_channel'))
    channels = get(publicChannels)
    print("Found {0} Public Channels".format(len(channels)))
    sleep(1)

    privateChannels = functools.partial(client.conversations_list, types=('private_channel,mpim'))
    groups = get(privateChannels)
    print("Found {0} Private Channels or Group DMs".format(len(groups)))
    # need to retrieve channel memberships for the slack-export-viewer to work
    for n in range(len(groups)):
        group = groups[n]
        members = functools.partial(client.conversations_members, channel=group['id'])
        m = get(members)
        group["members"] = m
        print("Retrieved {0} members of {1}".format(len(m), group['name']))
    sleep(1)

    dm = functools.partial(client.conversations_list, types=('im'))
    dms = get(dm)
    print("Found {0} 1:1 DM conversations\n".format(len(dms)))
    sleep(1)

    getUserMap()

# Returns the conversations to download based on the command-line arguments
def selectConversations(allConversations, commandLineArg, filter, prompt):
    global args
    if isinstance(commandLineArg, list) and len(commandLineArg) > 0:
        return filter(allConversations, commandLineArg)
    elif commandLineArg != None or not anyConversationsSpecified():
        if args.prompt:
            return prompt(allConversations)
        else:
            return allConversations
    else:
        return []

# Returns true if any conversations were specified on the command line
def anyConversationsSpecified():
    global args
    return args.publicChannels != None or args.groups != None or args.directMessages != None

# This method is used in order to create a empty Channel if you do not export public channels
# otherwise, the viewer will error and not show the root screen. Rather than forking the editor, I work with it.
def dumpDummyChannel():
    channelName = channels[0]['name']
    mkdir( channelName )
    fileDate = '{:%Y-%m-%d}'.format(datetime.today())
    outFileName = u'{room}/{file}.json'.format( room = channelName, file = fileDate )
    writeMessageFile(outFileName, [])

def finalize():
    os.chdir('..')
    output = ""
    if zipName:
        shutil.make_archive(zipName, 'zip', outputDirectory, None)
        shutil.rmtree(outputDirectory)
        output = zipName + ".zip"
    else:
        output = outputDirectory
    print("Created: " + output)
    print("typical next step: slack-export-viewer -z {logs}".format(logs=output))
    #TODO see if we can do it with api auth.revoke
    print("Go back to https://api.slack.com/apps/, select your app, click `Basic Information`, scroll to the bottom and click the red 'Delete App' button")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Export Slack history')

    parser.add_argument('--token', required=True, help="Slack API token")
    parser.add_argument('--zip', help="Name of a zip file to output as")
    #parser.add_argument('--files', help="Fetch files also (careful big and slow)")

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

    users = []    
    channels = []
    groups = []
    dms = []
    userNamesById = {}
    userIdsByName = {}
    client = WebClient(token=args.token)

    testAuth = doTestAuth()
    tokenOwnerId = testAuth['user_id']

    bootstrapKeyValues()

    dryRun = args.dryRun
    zipName = args.zip

    outputDirectory = "{0}-slack_export".format(datetime.today().strftime("%Y%m%d-%H%M%S"))
    mkdir(outputDirectory)
    os.chdir(outputDirectory)

    if not dryRun:
        dumpUserFile()
        dumpChannelFile()

    selectedChannels = selectConversations(
        channels,
        args.publicChannels,
        filterConversationsByName,
        promptForPublicChannels)

    selectedGroups = selectConversations(
        groups,
        args.groups,
        filterConversationsByName,
        promptForGroups)

    selectedDms = selectConversations(
        dms,
        args.directMessages,
        filterDirectMessagesByUserNameOrId,
        promptForDirectMessages)

    if len(selectedChannels) > 0:
        fetchPublicChannels(selectedChannels)

    if len(selectedGroups) > 0:
        if len(selectedChannels) == 0:
            dumpDummyChannel()
        fetchGroups(selectedGroups)

    if len(selectedDms) > 0:
        fetchDirectMessages(selectedDms)

    finalize()
