#!/usr/bin/env python3

import sys
import socket
import select
import time
import json
import random
import copy
import threading
import math
import zlib
import binascii

FOLLOWER = "follower"
CANDIDATE = "candidate"
LEADER = "leader"

REQUEST_VOTE_RPC = "requestVote"
APPEND_ENTRIES_RPC = "append_log"
VOTE = "vote"
REDIRECT = "redirect"
SRC = "src"
DST = "dst"
TYPE = "type"
LOG = "log"
TERM = "term"
KEY = "key"
VALUE = "value"
INDEX = "index"
LOG_MATCH = "log_match"
CONFLICT_TERM = "conflict_term"
CONFLICT_INDEX = "conflict_index"
LAST_INDEX = "last_index"
LAST_INDEX_TERM = "last_index_term"
ACCEPT = "accept"
MATCH_INDEX = "matchIndex"
COMMIT_INDEX = "commitIndex"
MID = "MID"
NO_LEADER = "FFFF"
ACK_HEARTBEAT = "ack_heartbeat"
ACK_APPEND_ENTRIES_RPC = "ack"
GET = "get"
PUT = "put"
ANSWERED = "answered"
LAST_LOG_INDEX = "last_log_index"


class KeyValueStore:

    def __init__(self, my_id, replica_ids):
        self.my_id = my_id
        self.replica_ids = replica_ids
        self.logs = []  # a single log -> {log, term, index}
        self.state_machine = {}

        # for all servers
        # highest log entry index known to be committed
        self.commit_index = 0
        # highest log entry index applied to the state machine
        # can get last log index and last log term --> for request vote rpc
        # last log = logs[self.last_applied] -> [term]
        self.last_applied = {INDEX: 0}

        # for leader
        # index of highest log entry known to be replicated
        self.match_index = {}
        # index of the next log entry to send send to other servers
        self.next_index = {}
        self.waiting = False
        self.followers_response = {}
        # for leader to send out heart beat
        self.last_heartbeat = 0
        self.last_sent_time = 0

        self.term = 0
        self.state = FOLLOWER
        self.vote_to = NO_LEADER
        self.leader_id = NO_LEADER
        self.received_votes = 0
        # for leader
        self.append_entries_ack = 0
        # for follower to check if leader is died
        self.last_time = 0

        # create a socket connection
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_SEQPACKET)
        self.sock.connect(my_id)
        self.election_timeout = None
        self.set_new_election_time_out()

        self.follower_not_match = []

        # for debug
        self.total_received_get = 0
        self.total_received_put = 0
        self.total_answered_get = 0
        self.total_answered_put = 0

    def set_new_election_time_out(self):
        self.election_timeout = (random.randrange(200, 600)) / 1000
        self.sock.settimeout(self.election_timeout)
        # print("timeout", self.my_id, "//", self.election_timeout)

    def keep_alive(self):
        now_time = time.time()
        time_difference = now_time - self.last_heartbeat
        if time_difference >= 0.15:
            # print("heartbeat, waiting: ", self.waiting,
            #       "last send time difference: ", time.time() - self.last_sent_time)
            self.parallel_heartbeat("empty")
            self.last_heartbeat = time.time()

    # leader send out heartbeat or append lof entry
    # append entry should depend on {match index}, and {next index}
    def heartbeat(self, replica_id, flag):
        self_last_index, self_last_index_term = self.return_last_index_term()
        log_entries = []

        if flag != "empty" or replica_id not in self.follower_not_match:
            next_index = self.next_index[replica_id]
            log_entries = self.logs[next_index:]

            if self.commit_index < next_index:
                log_entries = self.logs[self.commit_index:]
                self.next_index[replica_id] = self.commit_index

            if self.commit_index >= self.next_index[replica_id] or (replica_id not in self.follower_not_match):
                logs_len = len(self.logs[self.next_index[replica_id]:])
                self.next_index[replica_id] += logs_len

        # log entry may None if no customer request
        msg = {"src": self.my_id, "dst": replica_id, "leader": self.my_id, "type": APPEND_ENTRIES_RPC,
               "log": log_entries, "term": self.term, COMMIT_INDEX: self.commit_index,
               LAST_INDEX_TERM: self_last_index_term, LAST_INDEX: self_last_index}

        if replica_id not in self.follower_not_match:
            self.sock.send(json.dumps(msg).encode())

        return

    def find_match_log(self, replica_id, an_entry, last_log):
        last_index = last_log[INDEX]
        last_term = last_log[TERM]

        msg = {"src": self.my_id, "dst": replica_id, "leader": self.my_id, "type": APPEND_ENTRIES_RPC,
               "log": an_entry, "term": self.term, COMMIT_INDEX: self.commit_index,
               LAST_INDEX_TERM: last_term, LAST_INDEX: last_index}

        self.sock.send(json.dumps(msg).encode())
        return

    def append_log_entries(self, replica_id, log_entries):
        self_last_index, self_last_index_term = self.return_last_index_term()

        msg = {"src": self.my_id, "dst": replica_id, "leader": self.my_id, "type": APPEND_ENTRIES_RPC,
               "log": log_entries, "term": self.term, COMMIT_INDEX: self.commit_index,
               LAST_INDEX_TERM: self_last_index_term, LAST_INDEX: self_last_index}

        self.sock.send(json.dumps(msg).encode())
        return

    def parallel_heartbeat(self, flag):
        t1 = threading.Thread(target=self.heartbeat, args=(self.replica_ids[0], flag))
        t2 = threading.Thread(target=self.heartbeat, args=(self.replica_ids[1], flag))
        t3 = threading.Thread(target=self.heartbeat, args=(self.replica_ids[2], flag))
        t4 = threading.Thread(target=self.heartbeat, args=(self.replica_ids[3], flag))

        t1.start()
        t2.start()
        t3.start()
        t4.start()

        t1.join()
        t2.join()
        t3.join()
        t4.join()
        return

    # Initialization
    def reinitialize(self):
        for replica in self.replica_ids:
            self.next_index[replica] = len(self.logs)
            self.match_index[replica] = 0
            self.followers_response[replica] = False
            self.append_entries_ack = 0
            self.waiting = False
            self.follower_not_match = []

    # a replica as a leader
    # if receive a leader's msg with the same term, send back reject
    def leader(self):
        # print(f"I am the new leader: {self.my_id}, term: {self.term}")
        self.reinitialize()
        self.last_heartbeat = time.time()
        self.parallel_heartbeat("empty")

        while True:

            self.handel_leader_send()

            ready = select.select([self.sock], [], [], 0.1)[0]
            if self.sock in ready:
                msg_raw = self.sock.recv(32768)

                if len(msg_raw) == 0:
                    continue

                msg = json.loads(msg_raw.decode())
                msg_type = msg[TYPE]
                msg_from = msg[SRC]

                if msg_type in [GET, PUT]:
                    if msg_type == GET:
                        self.leader_response_client(msg)
                    elif msg_type == PUT:
                        # index used to identity the position in the logs
                        index = self.logs[-1][INDEX] + 1 if len(self.logs) > 0 else 0
                        # term used to detect in consistence
                        current_term = self.term
                        key = msg[KEY]
                        value = msg[VALUE]
                        client_id = msg_from
                        mid = msg[MID]

                        # a log entry for a put command
                        log_entry = {TERM: current_term, INDEX: index, KEY: key, VALUE: value, SRC: client_id, MID: mid}

                        self.logs.append(log_entry)

                        # send append entries if got majority for last append entries
                        # if still waiting for the ack just pass
                        if not self.waiting:
                            self.waiting = True
                            self.last_heartbeat = time.time()
                            self.last_sent_time = time.time()
                            for a_replica in self.replica_ids:
                                self.followers_response[a_replica] = False
                            self.parallel_heartbeat("logs")

                # receive from follower's ack
                elif msg_type == ACK_HEARTBEAT:
                    continue
                elif msg_type == ACK_APPEND_ENTRIES_RPC:
                    # accept
                    # print(f">>>ACK from {msg_from}")
                    # print(msg)
                    if msg[ACCEPT]:
                        if msg[LOG_MATCH]:

                            if msg_from in self.follower_not_match:
                                # print("--------------------start match index--------------------")
                                start_index = self.next_index[msg_from] + 1
                                log_entries = self.logs[start_index:self.commit_index]

                                log_sent = 0
                                size = 45
                                while log_sent < len(log_entries):
                                    upper = log_sent + size
                                    if upper > len(log_entries):
                                        upper = len(log_entries)
                                    self.append_log_entries(msg_from, log_entries[log_sent:upper])
                                    log_sent = upper

                                self.follower_not_match.remove(msg_from)
                                self.next_index[msg_from] = self.commit_index

                            if len(msg[LOG]) > 0:
                                # print(f"$$$$$$$$$$$$$$$$$$$$$$$-----> leader: {self.my_id}, commit index,"
                                #       f"{self.commit_index},follower first log entry index: {msg[LOG][0][INDEX]}, "
                                #       f"from {msg_from}")
                                if msg[LOG][0][INDEX] == self.commit_index:
                                    self.append_entries_ack += 1
                                    self.followers_response[msg_from] = True
                                self.update_match_index(msg)
                                # print(f"* Match index updated to: {self.match_index}")
                        else:
                            if msg_from not in self.follower_not_match:
                                self.follower_not_match.append(msg_from)

                            # print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n")
                            # print(f"leader ID: {self.my_id}, follower has mismatch log: {msg_from}")
                            # print("\n!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
                            # if log not match, decrement the next index
                            # until the index of log match on both leader and follower side
                            # resend the log entries to the server
                            self.next_index[msg_from] -= 1
                            next_index = self.next_index[msg_from]
                            the_log = [self.logs[next_index]]
                            last_log = {}
                            if self.logs[next_index][INDEX] > 0:
                                last_log = self.logs[next_index - 1]
                            elif self.logs[next_index][INDEX] == 0:
                                last_log = self.logs[0]

                            self.find_match_log(msg_from, the_log, last_log)

                        #     print(f"* Next index updated to: {self.next_index}")
                        # print(f"* I am the leader {self.my_id}, my commitIndex is {self.commit_index}")
                # may receive from other leader after partition being removed
                # current leader may valid/invalid
                elif msg_type == APPEND_ENTRIES_RPC:
                    accept_new_leader = False

                    if msg[TERM] > self.term:
                        accept_new_leader = True

                    # other leader term <= self term
                    if msg[COMMIT_INDEX] > self.commit_index and \
                            msg[LAST_INDEX_TERM] > self.logs[-1][TERM] and \
                            msg[LAST_INDEX] > len(self.logs) - 1:
                        accept_new_leader = True

                    if accept_new_leader:
                        # become to follower if self term < other leader term
                        self.leader_id = msg_from
                        self.state = FOLLOWER
                        self.term = msg[TERM]
                        self.received_votes = 0
                        self.set_new_election_time_out()
                        self.last_time = time.time()
                        # print(f"Leader: {self.my_id} accept leader: {msg_from},
                        # then became to follower: {self.my_id}")
                        # print(f"msg old leader received from new leader: ", msg)
                        self.response_to_leader(msg)
                        self.follower()
                        return

                elif msg_type == REQUEST_VOTE_RPC:
                    # receive other candidate request vote

                    send_out_message = {"src": self.my_id, "dst": msg_from, "leader": NO_LEADER, "term": self.term,
                                        COMMIT_INDEX: self.commit_index, TYPE: VOTE, VOTE: 1}

                    self_last_term_copy = 0
                    if len(self.logs) > 0:
                        self_last_term_copy = self.logs[-1][TERM]

                    if msg[TERM] > self.term and \
                            msg[TERM] > self_last_term_copy and \
                            msg[LAST_INDEX] >= len(self.logs) - 1 and \
                            msg[LAST_INDEX_TERM] >= self_last_term_copy:

                        self.state = FOLLOWER
                        self.vote_to = msg[SRC]
                        self.received_votes = 0
                        self.set_new_election_time_out()
                        self.last_time = time.time()
                        self.term = msg[TERM]
                        self.sock.send(json.dumps(send_out_message).encode())
                        # print(f"Leader:{self.my_id}, voted to: {msg_from}, then became to follower: {self.my_id}")
                        self.follower()
                        return
                    else:
                        self.term = msg[TERM] + 1
                        self.heartbeat(msg_from, "empty")

    def handel_leader_send(self):
        # if time out send empty heart beat to keep leader authority
        self.keep_alive()

        # if receive majority success response, write all log to state machine, response client
        if self.append_entries_ack >= 2:
            self.update_commit_index()

            # overwrite last applied
            self.append_entries_ack = 0
            self.waiting = False

            for index in range(self.last_applied[INDEX] + 1, self.commit_index + 1):
                a_log = self.logs[index]
                key = a_log[KEY]
                value = a_log[VALUE]
                self.state_machine[key] = value
                self.response_put(a_log)
                self.last_applied = a_log

            # self.commit_index += 1

        # if > 600ms not received success response, packets may dropped due to bad network
        # resend to follower who did not response
        current_time = time.time()
        if current_time - self.last_sent_time >= 0.6 and self.waiting:
            # send append entries to followers who did not response
            # print("----------------------------------------------")
            # print("majority packets dropped, not ack followers: ", self.followers_response)
            # print("----------------------------------------------")

            for a_follower in self.replica_ids:
                response = self.followers_response[a_follower]
                if not response:
                    self.heartbeat(a_follower, "logs")
            self.last_sent_time = time.time()

    def resend_drop_packets(self):
        # resend from match index to next index
        pass

    def update_match_index(self, msg):
        msg_from = msg[SRC]
        self.match_index[msg_from] = msg[LAST_LOG_INDEX]

    def update_commit_index(self):
        last_log_indexes = list(self.match_index.values()) + [len(self.logs) - 1]
        self.commit_index = sorted(last_log_indexes)[2]

    def leader_response_client(self, log_entry):
        request_ype = log_entry[TYPE]
        if request_ype == GET:
            self.response_get(log_entry)
        elif request_ype == PUT:
            self.response_put(log_entry)

    def response_get(self, log_entry):
        key = log_entry[KEY]
        new_msg = {"src": self.my_id, "dst": log_entry["src"], "leader": self.my_id, "type": "fail",
                   "MID": log_entry["MID"]}
        if key in self.state_machine:
            value = self.state_machine[key]
            new_msg[TYPE] = "ok"
            new_msg[VALUE] = value
        self.sock.send(json.dumps(new_msg).encode())
        return

    def response_put(self, log_entry):
        new_msg = {"src": self.my_id, "dst": log_entry["src"], "leader": self.my_id, "type": "ok",
                   "MID": log_entry["MID"]}
        self.sock.send(json.dumps(new_msg).encode())
        return

    # a replica as a candidate
    def candidate(self):
        # print("+++++++++++++++++++++++++++++++++")
        # print(f"I am the new candidate: {self.my_id}, term: {self.term}")
        # print("+++++++++++++++++++++++++++++++++")
        while True:
            if len(self.logs) != 0:
                is_ready = select.select([self.sock], [], [], 0.1)[0]
                ready = self.sock in is_ready
            else:
                ready = True
            if ready:
                try:
                    msg_raw = self.sock.recv(32768)
                except socket.timeout:
                    # this only happens when the split votes happened, multiple candidates exist
                    # time out is being reset
                    # candidate start a new term, received votes = 1,request votes again
                    self.term += 1
                    self.vote_to = self.my_id
                    self.received_votes = 1
                    self.send_request_vote()
                    # print(f"I am candidate: {self.my_id}, I timeout again: {self.term}")
                else:
                    received_msg = json.loads(msg_raw.decode())
                    msg_type = received_msg[TYPE]
                    msg_from = received_msg[SRC]

                    # msg from the client, redirect the msg
                    # will client msg stop candidate from timeout???
                    if msg_from not in self.replica_ids:
                        self.redirect_client_msg(received_msg)
                        continue

                    send_out_message = {"src": self.my_id, "dst": msg_from, "leader": "FFFF", "type": None,
                                        "term": self.term, COMMIT_INDEX: self.commit_index}

                    other_term = received_msg[TERM]
                    self_last_index, self_last_index_term = self.return_last_index_term()

                    if msg_type == REQUEST_VOTE_RPC:  # already vote for self
                        other_last_index = received_msg[LAST_INDEX]
                        other_last_index_term = received_msg[LAST_INDEX_TERM]

                        send_out_message["type"] = VOTE
                        send_out_message["vote"] = 0

                        if other_last_index_term == self_last_index_term:
                            # other candidate appear in the same term
                            if self_last_index == other_last_index:
                                if other_term > self.term:
                                    send_out_message["vote"] = 1
                                elif other_term == self.term:
                                    self.set_new_election_time_out()
                                    continue
                            elif other_last_index > self_last_index:
                                # vote for other candidate
                                send_out_message["vote"] = 1
                        elif other_last_index_term > self_last_index_term:
                            if other_last_index > self_last_index or \
                                    other_last_index == self_last_index:
                                send_out_message["vote"] = 1

                        self.sock.send(json.dumps(send_out_message).encode())

                        # if vote to other, become to follower state
                        if send_out_message["vote"] == 1:
                            # print(f"I am candidate: {self.my_id}, I vote to: {msg_from}")
                            self.candidate_to_follower(other_term, msg_from)
                            self.follower()
                            return

                    elif msg_type == VOTE:
                        vote_from_other = received_msg[VOTE]
                        self.received_votes += vote_from_other

                        # got 3 votes, current candidate able to be a leader.
                        if self.received_votes == 3:
                            self.leader_id = self.my_id
                            self.state = LEADER
                            self.sock.settimeout(None)
                            self.leader()
                            return
                    elif msg_type == APPEND_ENTRIES_RPC:
                        other_last_index = received_msg[LAST_INDEX]
                        other_last_index_term = received_msg[LAST_INDEX_TERM]
                        accept = False

                        if received_msg[TERM] >= self.term:
                            accept = True
                        else:
                            # receive form valid leader (accept)
                            if other_last_index >= self_last_index or other_last_index_term >= self_last_index_term:
                                accept = True

                        if not accept:
                            send_out_message[ACCEPT] = False
                            send_out_message[LOG] = received_msg[LOG]
                            if received_msg[LOG]:
                                send_out_message[TYPE] = ACK_APPEND_ENTRIES_RPC
                            else:
                                send_out_message[TYPE] = ACK_HEARTBEAT

                            self.sock.send(json.dumps(send_out_message).encode())
                            continue

                        # become to follower
                        self.leader_id = msg_from
                        self.candidate_to_follower(other_term, msg_from)
                        self.handel_follower_response(received_msg)
                        self.follower()
                        return

    def candidate_to_follower(self, other_term, msg_from):
        self.term = other_term
        self.state = FOLLOWER
        self.vote_to = msg_from
        self.received_votes = 0

    # candidate send out vote request
    def send_request_vote(self):
        last_index, last_term = self.return_last_index_term()
        request_vote_msg = {"src": self.my_id, "dst": None, "leader": NO_LEADER, "type": REQUEST_VOTE_RPC,
                            "term": self.term, LAST_INDEX: last_index,
                            LAST_INDEX_TERM: last_term}
        for a_replica in self.replica_ids:
            request_vote_msg["dst"] = a_replica
            self.sock.send(json.dumps(request_vote_msg).encode())
        return

    # a replica as a follower
    # if receive a leader's msg which is not the leader current follower vote to reject
    def follower(self):
        while True:
            if len(self.logs) != 0:
                is_ready = select.select([self.sock], [], [], 0.1)[0]
                ready = self.sock in is_ready
            else:
                ready = True

            if ready:
                try:
                    msg_raw = self.sock.recv(32768)
                except socket.timeout:
                    self.follower_election_timeout()
                    return
                else:
                    received_msg = json.loads(msg_raw.decode())
                    msg_type = received_msg[TYPE]
                    msg_from = received_msg[SRC]

                    # msg from the client, redirect the msg
                    if msg_from not in self.replica_ids:
                        # check if election time out
                        if self.last_time != 0:
                            time_right_now = time.time()
                            time_difference = time_right_now - self.last_time

                            if time_difference >= self.election_timeout:
                                # follower did not hear back from leader for election timeout
                                self.leader_id = NO_LEADER
                                self.redirect_client_msg(received_msg)
                                self.follower_election_timeout()
                                return

                            # print(f"+++++++ Follower redirect normal: {self.my_id}, my leader is  {self.leader_id},"
                            #     f"time difference: {time_difference}, my timeout: {self.election_timeout} ")

                        self.redirect_client_msg(received_msg)
                        continue

                    send_out_message = {"src": self.my_id, "dst": msg_from, "leader": "FFFF", "type": None,
                                        "term": self.term, COMMIT_INDEX: self.commit_index}

                    other_term = received_msg[TERM]

                    if msg_type == REQUEST_VOTE_RPC:

                        send_out_message["type"] = VOTE
                        send_out_message["vote"] = 0

                        other_last_index = received_msg[LAST_INDEX]
                        other_last_index_term = received_msg[LAST_INDEX_TERM]
                        self_last_index, self_last_index_term = self.return_last_index_term()

                        if self_last_index_term < other_last_index_term:
                            # from legit candidate
                            send_out_message["vote"] = 1
                        elif self_last_index_term == other_last_index_term:
                            # from legit candidate
                            if other_last_index > self_last_index or other_term > self.term:
                                send_out_message["vote"] = 1

                        if send_out_message[VOTE] == 1:
                            # print(f"I am follower: {self.my_id}, I voted to: {msg_from}")
                            self.last_time = time.time()
                            self.leader_id = NO_LEADER
                            self.term = other_term
                            self.vote_to = msg_from

                        self.sock.send(json.dumps(send_out_message).encode())

                    elif msg_type == APPEND_ENTRIES_RPC:
                        self.handel_follower_response(received_msg)
                        self.last_time = time.time()

    def handel_follower_response(self, msg):
        msg_from = msg[SRC]
        other_term = msg[TERM]
        accept_leader = False
        ack_to_invalid_leader = {"src": self.my_id, "dst": msg_from, "leader": self.leader_id, "term": self.term,
                                 "type": None, "log": msg[LOG], "accept": False}

        other_last_index = msg[LAST_INDEX]
        other_last_index_term = msg[LAST_INDEX_TERM]
        self_last_index, self_last_index_term = self.return_last_index_term()

        if len(self.logs) == 0 and self.leader_id == NO_LEADER:
            self.leader_id = msg_from
            accept_leader = True
        else:
            if other_term > self.term:
                accept_leader = True
            elif other_term < self.term:
                if self_last_index_term < other_last_index_term:
                    accept_leader = True
                elif self_last_index_term == other_last_index_term:
                    if other_last_index > self_last_index:
                        accept_leader = True
            elif other_term == self.term:
                accept_leader = True

        # follower receive from invalid leader
        if not accept_leader:
            if len(msg[LOG]) > 0:
                ack_to_invalid_leader[TYPE] = ACK_HEARTBEAT
            else:
                ack_to_invalid_leader[TYPE] = ACK_APPEND_ENTRIES_RPC

            self.sock.send(json.dumps(ack_to_invalid_leader).encode())
            return

        # follower receive from valid leader
        self.leader_id = msg_from
        self.term = other_term
        self.response_to_leader(msg)

    def response_to_leader(self, msg):
        ack_to_valid_leader = {"src": self.my_id, "dst": msg[SRC], "leader": self.leader_id,
                               "type": None, "log": msg[LOG], "accept": True, LOG_MATCH: True,
                               COMMIT_INDEX: self.commit_index, TERM: self.term}

        leader_last_index = msg[LAST_INDEX]
        leader_last_index_term = msg[LAST_INDEX_TERM]
        leader_commit_index = msg[COMMIT_INDEX]

        # receive an append entry rpc
        if len(msg[LOG]) > 0:
            ack_to_valid_leader[TYPE] = ACK_APPEND_ENTRIES_RPC

            # self term > leader's term
            if msg[TERM] < self.term:
                ack_to_valid_leader[ACCEPT] = False

            # If current follower don't have the previous log entry
            elif ((leader_last_index >= 0) and
                  (len(self.logs) > leader_last_index) and
                  (len(self.logs) > 0) and
                  self.logs[leader_last_index][TERM] != leader_last_index_term):
                ack_to_valid_leader[LOG_MATCH] = False

            # match log
            elif ((len(self.logs) > leader_last_index) and
                  (len(self.logs) > 0) and
                  (leader_last_index_term > self.logs[leader_last_index][TERM])):
                self.logs = self.logs[:leader_last_index + 1] + msg[LOG]

            # match log
            else:
                self.logs = self.logs[:leader_last_index + 1]
                self.logs += msg[LOG]

                msg[LOG] = list(filter(lambda log: leader_commit_index <= log[INDEX], msg[LOG]))
                ack_to_valid_leader[LOG] = msg[LOG]

                if len(msg[LOG]) > 0:
                    first_log = msg[LOG][0]
                    first_log_index = first_log[INDEX]
                    if first_log_index > leader_commit_index:
                        ack_to_valid_leader[LOG] = [self.logs[leader_last_index]] + msg[LOG]
                        #
                        # print("************************ DEBUG ************************")
                        # print(f"first send back index: {first_log_index}, "
                        #       f"leader commit index: {leader_commit_index}")
                        # print("************************ DEBUG ************************")

                highest_index = 0
                if len(self.logs) > 0:
                    highest_index = min(leader_commit_index, len(self.logs) - 1)

                self_index = copy.deepcopy(self.commit_index)

                for index in range(self_index, highest_index + 1):
                    a_put = self.logs[index]
                    self.state_machine[a_put[KEY]] = a_put[VALUE]
                    self.commit_index += 1
                    self.last_applied = a_put

            ack_to_valid_leader[LAST_LOG_INDEX] = self.logs[-1][INDEX]

            self.sock.send(json.dumps(ack_to_valid_leader).encode())
        return

    def follower_election_timeout(self):
        self.leader_id = NO_LEADER
        self.state = CANDIDATE
        self.term += 1
        self.vote_to = self.my_id
        self.received_votes = 1
        self.send_request_vote()
        self.candidate()

    # follower redirect client msg
    def redirect_client_msg(self, msg):
        redirect_msg = {"src": self.my_id, "dst": msg[SRC], "leader": self.leader_id, "type": REDIRECT,
                        "MID": msg[MID]}
        self.sock.send(json.dumps(redirect_msg).encode())
        return

    # utility method
    def return_last_index_term(self):
        last_index = 0
        last_term = 0
        if len(self.logs) > 0:
            last_index = self.logs[-1][INDEX]
            last_term = self.logs[-1][TERM]
        return last_index, last_term


########################################################################################################################

if __name__ == "__main__":
    # Read in id's
    # Your ID number
    read_my_id = sys.argv[1]

    # The ID numbers of all the other replicas
    read_replica_ids = sys.argv[2:]

    key_value_store = KeyValueStore(read_my_id, read_replica_ids)
    key_value_store.follower()
