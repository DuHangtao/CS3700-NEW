#!/usr/bin/env python3

import sys
import socket
import select
import time
import json
import random
import copy
import threading

FOLLOWER = "follower"
CANDIDATE = "candidate"
LEADER = "leader"


class KeyValueStore:

    def __init__(self, my_id, replica_ids):
        self.my_id = my_id
        self.replica_ids = replica_ids
        self.term = 0
        self.leader_id = "FFFF"
        self.state = FOLLOWER
        self.state_machine = {}
        self.logs = []
        self.current_index_to_be_committed = 0
        self.next_index_to_be_committed = 0

        # estimate the heart beat timeout
        self.heart_beat_timeout = 0

        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_SEQPACKET)
        # Connect to the network. All messages to/from other replicas and clients will occur over this socket
        self.sock.connect(my_id)
        self.vote = False
        self.vote_to = "FFFF"
        self.received_votes = 0
        self.election_timeout = None
        self.leader_alive = False
        self.set_new_election_time_out()

        self.total_received_get = 0
        self.total_received_put = 0
        self.total_answered_get = 0
        self.total_answered_put = 0
        # for leader
        self.empty_heartbeat_ack = 0
        self.append_entries_ack = -1
        #
        self.last_time = 0
        self.last_heartbeat = 0
        #
        self.buffered_put = []
        #
        self.follower_index = {}

    def set_new_election_time_out(self):
        self.election_timeout = (random.randrange(150, 300)) / 1000
        self.sock.settimeout(self.election_timeout)

    def parallel_heartbeat(self, replica_id, log_entry):
        # log entry may None if no customer request
        msg = {"src": self.my_id, "dst": replica_id, "leader": self.leader_id, "type": "heartbeat",
               "log": log_entry, "term": self.term, "current_index_to_be_committed": self.current_index_to_be_committed}
        if replica_id in self.follower_index:
            index = self.follower_index[replica_id]
            msg["missing_logs"] = self.logs[index + 1: self.current_index_to_be_committed]
            print("follower index:", index + 1)
            print("leader current_index_to_be_committed", self.current_index_to_be_committed)
            print("len missing logs", len(msg["missing_logs"]))
        # print("send out heart to:", replica_id)
        self.sock.send(json.dumps(msg).encode())

    def heartbeat(self, log_entry):
        t1 = threading.Thread(target=self.parallel_heartbeat, args=(self.replica_ids[0], log_entry))
        t2 = threading.Thread(target=self.parallel_heartbeat, args=(self.replica_ids[1], log_entry))
        t3 = threading.Thread(target=self.parallel_heartbeat, args=(self.replica_ids[2], log_entry))
        t4 = threading.Thread(target=self.parallel_heartbeat, args=(self.replica_ids[3], log_entry))

        t1.start()
        t2.start()
        t3.start()
        t4.start()

        t1.join()
        t2.join()
        t3.join()
        t4.join()
        self.follower_index = {}
        return

    # if no client request yet, send empty heart beat periodically to show self alive:
    # 1. handel ack(from empty heartbeat), if majority heartbeat ack receive, resend heartbeat
    # if got client request:
    # 1. write request to log
    # 2. send out log to all followers(log: put/get)
    # 3. expect response from followers:
    #    3.1 if majority (accepts) received
    #        -> write log to the state machine
    #        -> send back "ok" to client
    #    3.2 received most rejected -> (maybe?)try again
    # if not received enough ack dont heart beat, or send log to followers
    def leader_send_append_entries(self):
        self.sock.settimeout(None)
        append_entries_ack = 0

        # send the first heartbeat
        self.last_heartbeat = time.time()
        self.heartbeat("")

        # print("len of logs: ", len(self.logs))
        # print("next index: ", self.next_index_to_be_committed)

        while True:
            ready = select.select([self.sock], [], [], 0.1)[0]
            if self.sock in ready:
                msg_raw = self.sock.recv(32768)

                self.alive_heartbeat()

                if len(msg_raw) == 0:
                    continue

                msg = json.loads(msg_raw.decode())
                # print("message: ", msg)
                # print("I am leader: ", self.my_id, "my committed logs are: ", self.current_index_to_be_committed - 1)
                # print("\n")
                if msg["type"] in ["get", "put"]:

                    # add term number and current to be committed index to msg ensure the consistency
                    if msg['type'] == 'get':
                        self.total_received_get += 1

                        # print("total received get", self.total_received_get)
                        # print("leader receives get: \n", msg)

                        # do not consider the fail case for now
                        self.leader_response_client(msg)
                    elif msg['type'] == 'put':
                        msg["leader"] = self.my_id
                        msg["term"] = copy.deepcopy(self.term)
                        msg["index"] = copy.deepcopy(len(self.logs))
                        self.logs.append(msg)
                        self.total_received_put += 1
                        # print("total received put", self.total_received_put)
                        # print("leader receives put: \n", msg)
                elif msg["type"] == "ack_heartbeat":
                    # print("leader receives ack heartbeat: \n", msg)
                    # receive empty ack
                    self.empty_heartbeat_ack += 1
                elif msg["type"] == "ack":
                    # print("leader receives ack: \n", msg)
                    # print("leader current_index_to_be_committed: ", self.current_index_to_be_committed)
                    is_accept = msg["accept"]
                    received_log = msg["log"]
                    received_index = received_log["index"]
                    # receive append_entries ack
                    if not msg["match_index"]:
                        if msg["src"] not in self.follower_index.keys():
                            self.follower_index[msg["src"]] = msg["follower_index"]
                    if is_accept:
                        if received_index == self.current_index_to_be_committed:
                            append_entries_ack += 1
                elif msg["type"] in ["heartbeat"]:
                    #
                    # leader's current_index_to_be_committed may need to -1
                    #
                    other_committed_logs = msg["current_index_to_be_committed"]
                    if self.current_index_to_be_committed < other_committed_logs:
                        print("---------------------------------------\n\n\n\n\n")
                        print("Leader: ", self.my_id, "become to follower", "my term: ", self.term, "new leader is: ",
                              msg["src"], "my committed logs are: ", self.current_index_to_be_committed)
                        print("\n\n\n\n\n---------------------------------------")
                        self.term = msg["term"]
                        self.leader_id = msg["src"]
                        self.state = FOLLOWER
                        self.received_votes = 0
                        self.set_new_election_time_out()
                        if msg["log"]:
                            # ack Append Entry RPC
                            self.follower_response_append_entries(msg)
                        else:
                            # ack heartbeat
                            ack = {"src": self.my_id, "dst": msg["src"], "leader": self.leader_id,
                                   "type": "ack_heartbeat", "term": self.term, "accept": True}
                            self.sock.send(json.dumps(ack).encode())
                        self.handel_follower_response()
                        return

                if self.empty_heartbeat_ack == 4:
                    self.empty_heartbeat_ack = 0
                    if len(self.logs) > 0:
                        if len(self.logs) >= self.next_index_to_be_committed + 1:
                            log = copy.deepcopy(self.logs[self.next_index_to_be_committed])
                            # print("leader send append log to followers", log)
                            self.next_index_to_be_committed += 1
                            self.last_heartbeat = time.time()
                            self.heartbeat(log)

                elif append_entries_ack == 2:
                    append_entries_ack = 0
                    # (get) -> what if the key really does not exist
                    self.current_index_to_be_committed += 1
                    # print("leader current index to be committed: \n", self.current_index_to_be_committed)
                    log = self.logs[self.current_index_to_be_committed - 1]
                    self.leader_response_client(log)

                if len(self.logs) > 0:
                    if len(self.logs) >= self.next_index_to_be_committed + 1:
                        log = copy.deepcopy(self.logs[self.next_index_to_be_committed])
                        # print("leader send append log to followers", log)
                        self.last_heartbeat = time.time()
                        self.heartbeat(log)
                        self.next_index_to_be_committed += 1

    def alive_heartbeat(self):
        now_time = time.time()

        time_difference = (now_time - self.last_heartbeat) * 1.1
        if time_difference >= 0.15:
            self.empty_heartbeat_ack = 0
            self.last_heartbeat = time.time()
            self.heartbeat("")

    def leader_response_client(self, log):
        request_ype = log["type"]
        if request_ype == "get":
            self.get(log)
        elif request_ype == "put":
            self.put(log)

    def get(self, msg):
        key = msg["key"]
        new_msg = {"src": self.my_id, "dst": msg["src"], "leader": self.my_id, "type": "fail", "MID": msg["MID"]}
        if key in self.state_machine:
            value = self.state_machine[key]
            new_msg["type"] = "ok"
            new_msg["value"] = value
        self.total_answered_get += 1
        # print("total answered get: ", self.total_answered_get)
        # print("leader response get to client: \n", new_msg)
        self.sock.send(json.dumps(new_msg).encode())
        return

    def put(self, msg):
        key = msg["key"]
        value = msg["value"]
        self.state_machine[key] = value
        new_msg = {"src": self.my_id, "dst": msg["src"], "leader": self.my_id, "type": "ok", "MID": msg["MID"]}
        self.total_answered_put += 1
        # print("total answered put", self.total_answered_put)
        # print("leader response put to client: \n", new_msg)
        self.sock.send(json.dumps(new_msg).encode())
        return

    def follower_response_append_entries(self, msg):
        # compare term and index
        log = msg["log"]
        leader_term = log["term"]
        leader_log_index = log["index"]
        leader_id = msg["src"]

        ack = {"src": self.my_id, "dst": leader_id, "leader": self.leader_id,
               "type": "ack", "log": log, "accept": True, "match_index": True}

        if self.term == leader_term and len(self.logs) == leader_log_index:
            self.current_index_to_be_committed = msg["current_index_to_be_committed"] + 1
            # self.next_index_to_be_committed = msg["current_index_to_be_committed"]
            self.next_index_to_be_committed += 1
            key = log["key"]
            value = log["value"]
            self.state_machine[key] = value
            self.logs.append(log)

        else:
            print("-----------\n")
            print("follower id: ", self.my_id, "my log len: ", len(self.logs), "leader log len: ", leader_log_index)
            print("\n-----------")

            if "missing_logs" in msg.keys():
                highest_committed_index = self.current_index_to_be_committed - 1
                print("================================================")
                print("highest_committed_index: ", highest_committed_index)
                logs_to_be_added = msg["missing_logs"]
                print("len missing_logs: ", len(logs_to_be_added))
                self.logs[highest_committed_index:] = logs_to_be_added

                for a_buffered_put in self.buffered_put:
                    index = a_buffered_put["index"] - 1
                    highest_index = len(self.logs) - 1
                    print("a_buffered_put: ", index)
                    print("highest_index: ", highest_index)
                    if highest_index < index:
                        self.logs.append(a_buffered_put)
                # self.logs.extend(self.buffered_put)
                self.current_index_to_be_committed = leader_log_index + 1
                self.next_index_to_be_committed = leader_log_index + 1
                print("new log index:", log["index"] - 1)
                self.logs.append(log)

                for a_log in self.logs[highest_committed_index:]:
                    key = a_log["key"]
                    value = a_log["value"]
                    self.state_machine[key] = value

                print("I am follower:", self.my_id)
                print("my current index to be committed", self.current_index_to_be_committed)
                print("length of logs", len(self.logs))
                print("================================================")

                self.buffered_put = []

            else:
                ack["match_index"] = False
                ack["follower_index"] = self.current_index_to_be_committed - 1
                self.buffered_put.append(log)
        # print("follower ack: ", ack)
        self.sock.send(json.dumps(ack).encode())
        return

    def handel_follower_response(self):
        # try to receive message either from client or leader
        while True:
            ready = select.select([self.sock], [], [], 0.1)[0]
            if self.sock in ready:
                try:
                    msg_raw = self.sock.recv(32768)
                except socket.timeout:
                    # timeout become to candidate, start new election
                    self.follower_start_over_election()
                    return
                else:
                    msg = json.loads(msg_raw.decode())
                    # print("follower received", msg)
                    # print("I am: ", self.my_id, "my leader is:", self.leader_id)
                    # print("My committed logs are: ", self.current_index_to_be_committed - 1)
                    # print("\n")
                    from_who = msg["src"]
                    # if msg["term"] >= self.term:
                    if from_who in self.replica_ids:
                        self.last_time = time.time()

                        if msg["type"] == "request_vote":
                            print("I am follower/My term/committed index:", self.my_id, self.term,
                                  self.current_index_to_be_committed,
                                  "I received request vote from/term/committed index : ", from_who, msg["term"],
                                  msg["committed_logs"])

                            if msg["term"] > self.term:
                                if msg["committed_logs"] >= self.current_index_to_be_committed:
                                    print("I am follower:", self.my_id, "I vote to new candidate: ", from_who)
                                    self.leader_id = "FFFF"
                                    self.term = msg["term"]
                                    send_out_message = {"src": self.my_id, "dst": from_who, "leader": self.leader_id,
                                                        "type": "vote", "term": self.term, "vote": 1}
                                    self.vote_to = from_who
                                    # print("I am:", self.my_id, "new candidate exist, I vote to: ", from_who)
                                    self.sock.send(json.dumps(send_out_message).encode())
                                    self.new_leader_election()
                                    return

                        # response heartbeat or Append Entry RPC
                        elif msg["type"] == "heartbeat":
                            if msg["term"] >= self.term:

                                if msg["term"] > self.term:
                                    if msg["current_index_to_be_committed"] > self.current_index_to_be_committed:
                                        self.leader_id = from_who
                                        self.term = msg["term"]
                                    else:
                                        continue

                                if msg["log"]:
                                    # ack Append Entry RPC
                                    self.follower_response_append_entries(msg)
                                else:
                                    # ack heartbeat
                                    ack = {"src": self.my_id, "dst": from_who, "leader": self.leader_id,
                                           "type": "ack_heartbeat"}
                                    self.sock.send(json.dumps(ack).encode())
                            else:
                                print("I am follower/My term/committed index:", self.my_id, self.term,
                                      self.current_index_to_be_committed,
                                      "I received heartbeat from/term/committed index: ", from_who, msg["term"],
                                      msg["current_index_to_be_committed"])
                                if msg["current_index_to_be_committed"] > self.current_index_to_be_committed:
                                    self.leader_id = from_who
                                    self.term = msg["term"]

                                    if msg["log"]:
                                        # ack Append Entry RPC
                                        self.follower_response_append_entries(msg)
                                    else:
                                        # ack heartbeat
                                        ack = {"src": self.my_id, "dst": from_who, "leader": self.leader_id,
                                               "type": "ack_heartbeat"}
                                        self.sock.send(json.dumps(ack).encode())

                    else:
                        redirect_msg = {"src": self.my_id, "dst": from_who, "leader": self.leader_id,
                                        "type": "redirect",
                                        "MID": msg["MID"]}

                        if self.last_time != 0:
                            time_right_now = time.time()
                            time_difference = time_right_now - self.last_time
                            if time_difference > self.election_timeout:
                                self.leader_id = "FFFF"
                                self.sock.send(json.dumps(redirect_msg).encode())
                                self.follower_start_over_election()
                                return
                        # redirect the client
                        self.sock.send(json.dumps(redirect_msg).encode())

    def request_vote(self):
        request_vote_msg = {"src": self.my_id, "dst": None, "leader": self.leader_id, "type": "request_vote",
                            "term": self.term, "committed_logs": self.current_index_to_be_committed}
        for a_replica in self.replica_ids:
            request_vote_msg["dst"] = a_replica
            self.sock.send(json.dumps(request_vote_msg).encode())
        return

    def follower_start_over_election(self):
        print("++++++++++++++++++++++++++++++++++++++++++++++\n\n\n\n")
        print("I am the new candidate, partition exist: ", self.my_id)
        print("\n\n\n\n++++++++++++++++++++++++++++++++++++++++++++++")
        self.term += 1
        self.state = CANDIDATE
        self.vote_to = self.my_id
        self.received_votes = 1
        self.request_vote()
        self.new_leader_election()

    def new_leader_election(self):

        while True:
            try:
                raw_received_msg = self.sock.recv(32768)
            except socket.timeout:
                if self.state == FOLLOWER:
                    print("I am follower, I am timeout, I become to candidate: ", self.my_id)
                    self.state = CANDIDATE
                    self.term += 1
                    self.vote_self()
                    self.request_vote()
                elif self.state == CANDIDATE:
                    print("I am candidate, I am timeout, a new term start: ", self.my_id)
                    self.term += 1
                    self.vote = 1
                    self.vote_to = self.my_id
                    self.request_vote()
            else:
                received_msg = json.loads(raw_received_msg.decode())
                recv_msg_type = received_msg["type"]
                from_who = received_msg["src"]
                send_out_message = {"src": self.my_id, "dst": from_who, "leader": self.leader_id, "type": None,
                                    "term": self.term}

                if from_who not in self.replica_ids:
                    redirect_msg = {"src": self.my_id, "dst": from_who, "leader": "FFFF",
                                    "type": "redirect",
                                    "MID": received_msg["MID"]}
                    self.sock.send(json.dumps(redirect_msg).encode())
                    continue

                if recv_msg_type == "request_vote":
                    vote = 0
                    send_out_message["type"] = "vote"
                    if self.state == FOLLOWER:
                        if self.term == received_msg["term"]:
                            vote = self.vote_to_other(from_who)
                        if self.term < received_msg["term"]:
                            self.term = received_msg["term"]
                            self.vote = False
                            vote = self.vote_to_other(from_who)
                    elif self.state == CANDIDATE:
                        if received_msg["term"] == self.term:
                            self.set_new_election_time_out()
                        if received_msg["term"] > self.term:
                            self.term = received_msg["term"]
                            self.state = FOLLOWER
                            vote = 1
                            self.vote_to = from_who
                            self.received_votes = 0
                    send_out_message["vote"] = vote
                    self.sock.send(json.dumps(send_out_message).encode())
                elif recv_msg_type == "vote":
                    if self.state == CANDIDATE:
                        received_vote = received_msg["vote"]
                        self.received_votes += received_vote

                        if self.received_votes == 3:
                            self.leader_id = self.my_id
                            self.state = LEADER
                            print("I am the leader:", self.my_id)
                            self.leader_send_append_entries()
                            return
                elif recv_msg_type == "heartbeat":

                    send_out_message["type"] = "ack_heartbeat"
                    send_out_message["accept"] = True

                    if self.state == FOLLOWER:
                        self.term = received_msg["term"]
                        self.leader_id = from_who
                        print("I am follower: ", self.my_id, "my leader is: ", self.leader_id)

                    elif self.state == CANDIDATE:
                        if received_msg["term"] >= self.term:
                            self.term = received_msg["term"]
                            self.leader_id = from_who
                            self.state = FOLLOWER
                            self.received_votes = 0
                            print("I am follower: ", self.my_id, "my leader is: ", self.leader_id)
                        elif received_msg["term"] < self.term:
                            leader_committed_logs = received_msg["current_index_to_be_committed"]
                            if leader_committed_logs >= self.current_index_to_be_committed:
                                self.term = received_msg["term"]
                                self.leader_id = from_who
                                self.state = FOLLOWER
                                self.received_votes = 0
                                print("I am candidate to follower: ", self.my_id, "my leader is: ", self.leader_id)
                            else:
                                continue
                    self.sock.send(json.dumps(send_out_message).encode())
                    # go into follower mode for all cases for now
                    self.handel_follower_response()
                    return

    def candidate_admit_leader(self):
        # self.term = received_msg["term"]
        # self.leader_id = from_who
        # self.state = FOLLOWER
        # self.received_votes = 0
        pass

    def vote_self(self):
        if not self.vote:
            self.vote_to = self.my_id
            self.vote = True
            self.received_votes += 1
        return

    def vote_to_other(self, recv_from_id):
        if not self.vote:
            self.vote = True
            self.vote_to = recv_from_id
            return 1
        return 0

    def start(self):
        self.new_leader_election()


########################################################################################################################

if __name__ == "__main__":
    # Read in id's
    # Your ID number
    read_my_id = sys.argv[1]

    # The ID numbers of all the other replicas
    read_replica_ids = sys.argv[2:]

    key_value_store = KeyValueStore(read_my_id, read_replica_ids)
    key_value_store.start()
