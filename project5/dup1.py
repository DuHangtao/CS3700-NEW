#!/usr/bin/env python

import sys, socket, select, time, json, random, uuid, math, random


class ServerState:
    FOLLOWER = 0
    CANDIDATE = 1
    LEADER = 2


# Your ID number
my_id = sys.argv[1]
# The ID numbers of all the other replicas
replica_ids = sys.argv[2:]
# Connect to the network. All messages to/from other replicas and clients will
# occur over this socket
sock = socket.socket(socket.AF_UNIX, socket.SOCK_SEQPACKET)
sock.connect(my_id)

# Persistent state
state = ServerState.FOLLOWER
currentTerm = 0
votedFor = ''
log = []  # Stores commands in a tuple (term, key, value)
pendingClientRequests = []  # Cache any put requests while waiting for previous quourm
stateMap = {}  # Stores <key, value> which have been committed
leaderId = 'FFFF'
commitIndex = 0
lastApplied = 0
majority = 0
lastVotedTerm = None

# Leader state
nextIndex = {}
matchIndex = {}
quorumPending = False
quorumVotes = 0
quorumSentTime = 0  # For timeouts of  quorums
quorumTimeout = 30
quorumCurrentTxn = ""
quorumResponses = {}  # <id, bool> for who has responded success for quorum

# Timeouts in milliseconds
leaderHeartBeat = 150
electionTimeout = random.randint(250, 400)
lastReceived = int(round(time.time() * 1000))
lastHeartBeat = int(round(time.time() * 1000))

# Candidate state
numVotes = 0


# Initialization function
def initializeIndexes():
    global majority, nextIndex, matchIndex
    majority = math.ceil(len(replica_ids)) / 2
    for replica in replica_ids:
        nextIndex[replica] = len(log)
        matchIndex[replica] = 0
        quorumResponses[replica] = False


# Helper print statement with id and timestamp
def printWithId(statement):
    currTime = int(round(time.time() * 1000))


# print str(my_id) + "<" + str(currTime) + ">: " + str(statement)

# Should only be used by Leader to respond to any incomining gets or puts
def respondtoget(msg):
    if msg['key'] in stateMap:
        response = {
            'src': my_id,
            'dst': msg['src'],
            'leader': my_id,
            'type': 'ok',
            'MID': msg['MID'],
            'value': stateMap[msg['key']],
        }
    else:
        response = {
            'src': my_id,
            'dst': msg['src'],
            'leader': my_id,
            'type': 'ok',
            'MID': msg['MID'],
            'value': '',
        }

    sock.send(json.dumps(response))


# Used by leader to respond to put requests
def putResponse(msg):
    if msg['key'] in stateMap:
        response = {
            'src': my_id,
            'dst': msg['src'],
            'leader': my_id,
            'type': 'ok',
            'MID': msg['MID'],
            'value': stateMap[msg['key']],
        }
        sock.send(json.dumps(response))
    else:
        failrequest(msg)


# Append an entry to log
def appendPutEntry(msg):
    global currentTerm
    command = (currentTerm, msg['key'], msg['value'])
    log.append(command)


# Sends out appendentriesrpc messages and tries to get a majority
def putGetQuorum():
    global currentTerm, leaderId, log, commitIndex, lastheartBeat, leaderHeartBeat
    prevLogIndex = len(log) - 1
    prevLogTerm = 0
    if (len(log) > 0):
        prevLogTerm = log[-1][0]
    for rep_id in replica_ids:
        entries = log[nextIndex[rep_id]:]
        appendentriesrpc(entries, rep_id, prevLogIndex, prevLogTerm)


# Takes the index of a command in the log and commits it to the stateMap
def commitCommand(index):
    global commitIndex, log, stateMap
    command = log[index]
    stateMap[command[1]] = command[2]
    commitIndex += 1


# Redirects message requests to the leader
def redirectrequest(msg):
    response = {
        'src': my_id,
        'dst': msg['src'],
        'leader': leaderId,
        'type': 'redirect',
        'MID': msg['MID'],
        'value': "",
    }
    sock.send(json.dumps(response))


# Responds with a fail
def failrequest(msg):
    response = {
        'src': my_id,
        'dst': msg['src'],
        'leader': leaderId,
        'type': 'fail',
        'MID': msg['MID'],
        'value': "",
    }
    sock.send(json.dumps(response))


# Candidate requests votes from other replicas
def requestvotesrpc(term, candidateId):
    global log
    lastLogIndex = 0
    lastLogTerm = 0
    if (len(log) > 0):
        lastLogIndex = len(log) - 1
        lastLogTerm = log[lastLogIndex][0]
    mid = str(uuid.uuid4())
    sock.send(json.dumps({
        'src': my_id,
        'dst': 'FFFF',
        'leader': my_id,
        'type': 'requestvotesrpc',
        'args': {
            'term': term,
            'candidateId': candidateId,
            'lastLogIndex': lastLogIndex,
            'lastLogTerm': lastLogTerm,
        },
        'MID': mid,
    }))


# Follower cast vote logic
def castVote(msg):
    global currentTerm, votedFor, lastVotedTerm
    term = msg['args']['term']
    lastLogTerm = 0
    if (len(log) > 0):
        lastLogTerm = log[-1][0]

    if (term > currentTerm and term > lastVotedTerm and msg['args']['lastLogIndex'] >= len(log) - 1 and msg['args'][
        'lastLogTerm'] >= lastLogTerm):
        sock.send(json.dumps({
            'src': my_id,
            'dst': msg['src'],
            'leader': leaderId,
            'type': 'requestvotesresponse',
            'args': {
                'voteGranted': True,
                'term': currentTerm,
            },
        }))
        votedFor = msg['src']
        lastVotedTerm = term

    elif (term <= lastVotedTerm or votedFor != ''):
        sock.send(json.dumps({
            'src': my_id,
            'dst': msg['src'],
            'leader': leaderId,
            'type': 'requestvotesresponse',
            'args': {
                'voteGranted': False,
                'term': currentTerm,
            },
        }))


# Sends out an appendentriesrpc to followers/candidates
def appendentriesrpc(entries, dest, prevLogIndex, prevLogTerm, txnId=None):
    global currentTerm, leaderId, commitIndex

    sock.send(json.dumps({
        'src': my_id,
        'dst': dest,
        'leader': leaderId,
        'type': 'appendentriesrpc',
        'args': {
            'term': currentTerm,
            'leaderId': leaderId,
            'prevLogIndex': prevLogIndex,
            'prevLogTerm': prevLogTerm,
            'entries': entries,
            'leaderCommit': commitIndex,
            'txnId': str(txnId)
        }
    }))


# appends multiple command entries to the log
def appendentries(entries):
    global log
    log += entries

    return True


# commits entries up min(leaderIndex, len(log))
def commitentries(leaderIndex):
    index = 0
    if (len(log) > 0):
        index = min(leaderIndex, len(log) - 1)

    for x in range(commitIndex, index):
        commitCommand(x)

    return True


# Response for appendentriesrpc from follower. Relies on appendentries and commitentries usually
def appendentriesresponse(msg):
    global log, currentTerm
    ret = {
        'term': currentTerm,
        'txnId': msg['args']['txnId']
    }
    args = msg['args']

    # If we are ahead of the leader.
    if (args['term'] < currentTerm):
        ret['success'] = False
    # If we don't have the previous log entry (we are too far behind to append these new entries)

    elif (args['prevLogIndex'] >= 0 and len(log) > args['prevLogIndex'] and len(log) > 0 and log[args['prevLogIndex']][
        0] != args['prevLogTerm']):
        ret['success'] = False
    # If there is a fail, the leader must decrement the nextIndex entry for this replica, so it can send all necessary entries.

    elif (len(log) > args['prevLogIndex'] and len(log) > 0 and args['prevLogTerm'] > log[args['prevLogIndex']][0]):
        log = log[:args['prevLogIndex'] + 1] + args['entries']
        ret['success'] = True

    else:  # If we haven't failed, delete conflicting records, and append new ones.
        log = log[:args['prevLogIndex'] + 1]  # We can discard anything past what the leader sends us.
        appendentries(args['entries'])  # And append what the leader sent to our log
        ret['success'] = True
        commitentries(args['leaderCommit'])

    # Send response to leader.
    sock.send(json.dumps({
        'src': my_id,
        'dst': leaderId,
        'leader': leaderId,
        'type': 'appendentriesresponse',
        'args': ret
    }))


# Leader sends heartbeat and resets timer
def sendHeartbeat():
    global lastHeartBeat
    entries = []
    appendentriesrpc(entries, 'FFFF', 0, 0)
    lastHeartBeat = int(round(time.time() * 1000))


# Steps for candidate to concede election
def concedeElection(msg):
    global state, currentTern, numVotes, electionTimeout, lastReceived
    leaderId = msg['src']
    state = ServerState.FOLLOWER
    currentTerm = msg['args']['term']
    numVotes = 0
    electionTimeout = random.randint(250, 400)
    lastReceived = int(round(time.time() * 1000))


# Finds the highest value a quorum for exists
def matchIndexHighestQuorum():
    global matchIndex, nextIndex
    indexes = matchIndex.values()
    indexes.append(len(log))
    return sorted(indexes)[int(math.ceil(len(indexes) / 2))]


# Main loop
initializeIndexes()
while True:
    ready = select.select([sock], [], [], 0.025)[0]
    currTime = int(round(time.time() * 1000))
    if state == ServerState.FOLLOWER:
        # No response from leader, declare self as candidate
        if currTime - lastReceived >= electionTimeout:
            state = ServerState.CANDIDATE
            leaderId = 'FFFF'
            numVotes = 1
            lastReceived = int(round(time.time() * 1000))
            electionTimeout = random.randint(250, 400)
            requestvotesrpc(currentTerm + 1, my_id)

        for sock in ready:
            msg_raw = sock.recv(32768)

            if len(msg_raw) == 0:
                continue
            msg = json.loads(msg_raw)
            # redirect requests to leader
            if msg['type'] in ['get', 'put']:
                redirectrequest(msg)
            elif msg['type'] == 'appendentriesrpc':
                leaderId = msg['src']
                currentTerm = msg['args']['term']
                votedFor = ''
                lastVotedTerm = currentTerm
                lastReceived = currTime
                entries = msg['args']['entries']
                if (len(entries) > 0):
                    appendentriesresponse(msg)
            elif msg['type'] == 'requestvotesrpc':
                lastReceived = currTime
                castVote(msg)

    if state == ServerState.CANDIDATE:
        if (numVotes > majority):
            state = ServerState.LEADER
            initializeIndexes()
            leaderId = my_id
            currentTerm += 1
            sendHeartbeat()
            continue
        if currTime - lastReceived >= electionTimeout:
            electionTimeout = random.randint(300, 500)
            currentTerm += 1
            lastReceived = currTime
            numVotes = 0
            # TODO pass proper parameters
            requestvotesrpc(currentTerm + 1, my_id)
        for sock in ready:
            msg_raw = sock.recv(32768)
            if len(msg_raw) == 0: continue
            msg = json.loads(msg_raw)
            if msg['type'] in ['get', 'put']:
                failrequest(msg)
            elif msg['type'] == 'requestvotesrpc' and msg['args']['term'] > currentTerm:
                concedeElection(msg)
                castVote(msg)
            elif msg['type'] == 'requestvotesresponse':
                if msg['args']['voteGranted'] == True:
                    numVotes += 1
            elif msg['type'] == "appendentriesrpc" and msg['args']['term'] > currentTerm:
                concedeElection(msg)

    if state == ServerState.LEADER:
        if (currTime - lastHeartBeat > leaderHeartBeat):
            sendHeartbeat()

        if (quorumPending and currTime - quorumSentTime > quorumTimeout):
            for _id, status in quorumResponses.iteritems():
                if (not status):
                    index = nextIndex[_id]
                    appendentriesrpc(log[index:], id, index - 1, log[index - 1][0], quorumCurrentTxn)
            quorumSentTime = currTime

        if (quorumVotes >= majority):  # If quorum received, commit entries
            highestIndex = matchIndexHighestQuorum()
            for i in range(commitIndex, highestIndex + 1):
                commitCommand(i)
                if (len(pendingClientRequests) > 0):
                    putResponse(pendingClientRequests.pop(0))

            quorumPending = False
            quorumVotes = 0
            quorumCurrentTxn = ""

        if sock in ready:
            msg_raw = sock.recv(32768)
            if len(msg_raw) == 0:
                continue
            msg = json.loads(msg_raw)
            if msg['type'] == 'get':
                respondtoget(msg)
            if msg['type'] == 'put':
                # These two before quorum
                pendingClientRequests.append(msg)
                appendPutEntry(msg)

                if not quorumPending:
                    quorumCurrentTxn = str(uuid.uuid4())
                    quorumPending = True
                    quorumResponses = dict.fromkeys(quorumResponses, False)
                    quorumSentTime = currTime
                    for id, index in nextIndex.iteritems():
                        appendentriesrpc(log[index:], id, index - 1, log[index - 1][0],
                                         quorumCurrentTxn)  # Send correct RPC to each follower.
                        nextIndex[id] = len(log)
                    lastHeartBeat = int(round(time.time() * 1000))

            if msg['type'] == 'appendentriesresponse':  # Handle responses from followers.
                args = msg['args']
                if args['success']:
                    if args['txnId'] == quorumCurrentTxn:
                        quorumVotes += 1
                        quorumResponses[msg['src']] = True
                    matchIndex[msg['src']] = nextIndex[msg['src']] - 1
                else:
                    nextIndex[msg['src']] -= 1
                    next = nextIndex[msg['src']]
                    appendentriesrpc(log[next:], msg['src'], next - 1, log[next - 1][0], args['txnId'])

            if msg['type'] == 'appendentriesrpc' and msg['args']['term'] > currentTerm:
                concedeElection(msg)
            if msg['type'] == 'requestvotesrpc' and msg['args']['term'] > currentTerm:
                term = msg['args']['term']
                lastLogTerm = 0
                if (len(log) > 0):
                    lastLogTerm = log[-1][0]
                if msg['args']['lastLogIndex'] >= len(log) - 1 and msg['args']['lastLogTerm'] >= lastLogTerm:
                    state = ServerState.FOLLOWER
                    numVotes = 0
                    electionTimeout = random.randint(250, 400)
                    lastReceived = int(round(time.time() * 1000))
                    castVote(msg)
                else:
                    currentTerm = msg['args']['term'] + 1
                    sendHeartbeat()
