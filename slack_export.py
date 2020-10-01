import json
import argparse
import os
import io
import shutil
import copy
import sys
from datetime import datetime
from pick import pick
from time import sleep
from time import time
import functools
import tempfile
import glob
import random
import urllib

from slack import WebClient
from slack.errors import SlackApiError

#TODO make sure all the filepath / \ stuff is working in windows and linux.

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
    # if there's no data to write to the file, return
    if not messages:
        return
    directory = os.path.dirname(fileName)
    mkdir( directory )
    dumpJson(messages, fileName)

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
def fetchPublicChannels(channels, dryRun):
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

#May need this later if guessListDataKey starts failing with api change.
#def getMessages(func):
#    return get(func, datakey="messages")
#def getMembers(func):
#    return get(func, datakey="members")
#def getChannels(func):
#    return get(func, datakey="channels")

def get_pages(func, datakey=None):
    arr = []
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
    return arr

#Gather all the paged data from a SlackResponse
def get(func, datakey=None):
    arr = []
    success = False
    for f in range(3):
        try:
            arr = get_pages(func, datakey)
            #print("Success")
            success = True
        except SlackApiError as e:
            print(e)
            data = e.response.data
            if data['error'] == 'missing_scope':
                print("Your OAuth Token at http://api.slack.com/apps is missing permission:  " + data['needed'])
                print(data['needed'] + " permission can be added to the 'OAuth & Permissions' page, then click 'Reinstall App' at the top of the page.")
            exit(-1)
        except ConnectionResetError as e:
            if f == 2:
                raise
            print("Retry Connection")
            continue
        except urllib.error.URLError as e:
            if f == 2:
                raise
            print("Retry URL Error")
            continue
        if success:
            break
    return arr

def getThreadHistory(channel, ts):
    global client
    threadHistory = functools.partial(client.conversations_replies, channel=channel, ts=ts)
    msgs = get(threadHistory)
    #print("{0} msg thread history for {1} at {2}".format(len(msgs), channel, ts))
    return msgs

#consider option for oldest=ts or latest=ts to for a time window.
#https://api.slack.com/methods/conversations.history
def getChannelHistory(channel):
    global client
    channelHistory = functools.partial(client.conversations_history, channel=channel)
    msgs = get(channelHistory)
    #print("{0} msg history for {1}".format(len(msgs), channel))
    return msgs

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
def fetchDirectMessages(dms, dryRun):
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
def fetchGroups(groups, dryRun):
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



# get basic info about the slack channel to ensure the authentication token works
def doTestAuth():
    global client
    try:
        auth = client.auth_test()
        print("Successfully authenticated")
        for k in auth.data:
            if not k == 'ok':
                print(k + ": " + str(auth.data[k]))
    except SlackApiError:
        print("AUTHENTICATION FAILED!")
        print("Create a new app (or Reinstall an old app and copy a new token) at https://api.slack.com/apps")
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

def readCachedJson(file):
    global readTmpDir
    if readTmpDir:
        path = readTmpDir + '/' + file
        if os.path.exists(path):
            with open(path) as f:
                return json.load(f)
    return False

def dumpJson(obj, file):
    with open(file, 'w') as outFile:
        json.dump(obj , outFile, indent=4)

def dumpJsonAndCache(obj, filename):
    global tmpDir
    dumpJson(obj,filename)
    if tmpDir:
        shutil.copy(filename, tmpDir)

def copyCache(filename):
    global tmpDir
    if tmpDir:
        path = tmpDir + '/' + filename
        if os.path.exists(path):
            shutil.copy(path, filename)

def bootstrapKeyValues(tokenOwnerId):
    global tmpDir, users, channels, groups, dms, client

    #general pattern
    #1) check for a cached tmp json file (do not bother slack.com for something that changes rarely, that it told you minutes ago)
    #2) otherwise we get via slackclient.
    #2.1) dump to cached json file asap.  (Good for not repeatedly fetching users.json)
    #to always get from slack use --fresh.  TODO specify list of files to get fresh, TODO protection for canceling mid json write.

    src = "[cached]"
    users = readCachedJson('users.json')
    if users:
        #dumpJson(users, 'users.json')
        copyCache('users.json')
    else:
        src = "[slack.com]"
        users = get(client.users_list)
        dumpJsonAndCache(users, 'users.json')
    print("{0} {1} Users".format(src, len(users)))
    
    src = "[cached]"
    channels = readCachedJson('channels.json')
    if channels:
        copyCache('channels.json')
    else:
        src = "[slack.com]"
        publicChannels = functools.partial(client.conversations_list, types=('public_channel'))
        channels = get(publicChannels)
        dumpJsonAndCache(channels, 'channels.json')
    print("{0} {1} Public Channels".format(src, len(channels)))

    #Note privatChannels.json is equivalent to (groups.json + mpims.json)
    src = "[cached]"
    groups = readCachedJson('privateChannels.json')
    if groups:
        copyCache('privateChannels.json')
    else:
        src = "[slack.com]"
        privateChannels = functools.partial(client.conversations_list, types=('private_channel,mpim'))
        groups = get(privateChannels)
        # need to retrieve channel memberships for the slack-export-viewer to work
        for group in groups:
            members = functools.partial(client.conversations_members, channel=group['id'])
            m = get(members)
            group["members"] = m
            print("Retrieved {0} members of {1}".format(len(m), group['name']))
        dumpJsonAndCache(groups, 'privateChannels.json')
    print("{0} {1} Private Channels or Group DMs".format(src, len(groups)))

    #split groups into mpims and private regardless of source to avoid race conditions.
    private = []
    mpims = []
    for group in groups:
        if group['is_mpim']:
            mpims.append(group)
        else:
            private.append(group)
    dumpJson(private, 'groups.json')
    dumpJson(mpims, 'mpims.json')

    src = "[cached]"
    dms = readCachedJson('dms.json')
    if dms:
        copyCache('dms.json')
    else:
        src = "[slack.com]"
        dm = functools.partial(client.conversations_list, types=('im'))
        dms = get(dm)
        # slack-export-viewer wants DMs to have a members list, not sure why but doing as they expect
        for dm in dms:
            dm['members'] = [dm['user'], tokenOwnerId]
        dumpJsonAndCache(dms, 'dms.json')
    print("{0} {1} 1:1 DM conversations\n".format(src, len(dms)))

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

def spinner():
    syms = ['\\', '|', '/', '-']
    t = random.uniform(0.01, 0.15)
    for i in range(random.randint(10,30)):
        sys.stdout.write("\b%s" % syms[i%4])
        sys.stdout.flush()
        sleep(t)
    sys.stdout.write("\b")
    sys.stdout.flush()

def promptRevokeToken():
    title = 'Question: Would you like to protect your account and revoke the secret --token xoxp-<secret-password-hex> we just used?'
    options = [
        'Yes    - I want to protect my account, If I need to use it again I will visit https://api.slack.com/apps/, refresh the App OAuth & Permissions page, and Install App to Workspace',
        'No     - I just did a test run and will use the token again soon, I realise this is a little unsafe and will disable it next time',
        'Gamble - I do not understand the question, I refuse to look it up, lets roll the dice.'
    ]
    option, index = pick(options, title)
    if index == 2:
        index=0
        for d in range(6):
            spinner()
            r = random.randint(1,6)
            c = '.' if r<5 else '!'
            print("Rolled a {r}{c}".format(r=r,c=c))
            if r == 6:
                break
    doRevoke=(index==0)
    return doRevoke

def revokeToken():
    global client
    response = client.auth_revoke()
    if response['ok']:
        print("Token is revoked, visit https://api.slack.com/apps/, refresh the OAuth page, and Install App to Workspace")
    else:
        print(response)
        print("Failed to revoke")

def initialize():
    outputDirectory = "{0}-slack_export".format(datetime.today().strftime("%Y%m%d-%H%M%S"))
    mkdir(outputDirectory)
    os.chdir(outputDirectory)
    return outputDirectory

def finalize(zipName, outputDirectory):
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

    if promptRevokeToken():
        revokeToken()
    else:
        print("Token is still active! visit https://api.slack.com/apps/, select your app, click `Basic Information`, scroll to the bottom and click the red 'Delete App' button")
        print("Alternatively rerun this script with --revokeAccessDoNothing")

def reconnectClient(token):
    global client
    print("Reconnecting Client")
    client = None
    sleep(1)
    client = WebClient(token=token, timeout=180)

def Main(argsin):
    global client, userNamesById, userIdsByName, readTmpDir, tmpDir, channels, groups, dms, users, args
    args = argsin

    users = []
    channels = []
    groups = []
    dms = []
    userNamesById = {}
    userIdsByName = {}
    reconnectClient(args.token)
    testAuth = doTestAuth()
    tokenOwnerId = testAuth['user_id']

    if args.revokeAccessDoNothing:
        revokeToken()
        exit(0)

    #create a tmp dir to use between reruns.
    tmpDir = tempfile.gettempdir() + "/slack-export"
    mkdir(tmpDir)
    #We always write to tmpDir, but may not read sometimes.
    readTmpDir = tmpDir

    if args.fresh:
        print("Skipping any cached json files. [--fresh]")
        readTmpDir = None
    else:
        #delete anything too old.
        now = time()
        dayOfSeconds = 86400
        threshold = now - dayOfSeconds
        for f in glob.glob(tmpDir + "/*.json"):
            if os.stat(f).st_ctime < threshold:
                print("deleted" + f + " (too old)")
                os.remove(f)

    outputDirectory = initialize() #note chdir

    bootstrapKeyValues(tokenOwnerId)

    reconnectClient(args.token)

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
        fetchPublicChannels(selectedChannels, args.dryRun)
        reconnectClient(args.token)

    if len(selectedGroups) > 0:
        if len(selectedChannels) == 0:
            dumpDummyChannel()
        fetchGroups(selectedGroups, args.dryRun)
        reconnectClient(args.token)

    if len(selectedDms) > 0:
        fetchDirectMessages(selectedDms, args.dryRun)

    finalize(args.zip, outputDirectory)

def AllPrivateMessagesWrapper(token):
    class FakeArgs:
        def __init__(self, **kwargs):
            self.__dict__.update(kwargs)
    args = FakeArgs(prompt=False, zip=False, dryRun=False, fresh=False, revokeAccessDoNothing=False, 
                    token=token, publicChannels=None, groups=[], directMessages=[])
    Main(args)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Export Slack history')

    parser.add_argument('--token', required=True, help="Slack API token")
    parser.add_argument('--zip', help="Name of a zip file to output as")
    #parser.add_argument('--files', help="Fetch files also (careful big and slow)")

    parser.add_argument(
        '--fresh',
        action='store_true',
        default=False,
        help="Do not use temp files of users / channels (super tedious on reruns when you know the users/channels haven't changed)")

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

    parser.add_argument(
        '--revokeAccessDoNothing',
        action='store_true',
        default=False,
        help="revoke the --token given and exit, nothing is downloaded, token is locked out")

    args = parser.parse_args()

    Main(args)
    
