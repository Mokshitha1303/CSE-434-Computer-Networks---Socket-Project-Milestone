"""peer.py - DHT peer (UDP, single thread, select()).

Usage:
    python peer.py <manager-IPv4> <manager-port>

Typed commands (stdin):
    register <name> <ip> <m-port> <p-port>
    setup-dht <name> <n> <YYYY>
    dht-complete <name>          (the leader also sends this automatically)
    query-dht <name> <event_id>  (asks the manager for a node, then runs find-event)
    leave-dht <name>
    join-dht <name>
    teardown-dht <name>          (leader only)
    deregister <name>            (Free peers only; the process then exits)

Role in the protocol:
    * m-port socket: strict request/response with the manager.
    * p-port socket: peer traffic (set-id, store, count, reset, teardown,
      reset-id, rebuild-dht, find-event ...). Management messages always
      travel one way around the ring: 0 -> 1 -> ... -> n-1 -> 0.
    * One thread + select() over [stdin, p_sock] so a non-leader keeps
      forwarding ring traffic while still accepting typed commands.
      (select() cannot watch stdin on Windows, so there a tiny helper thread
      relays stdin lines to a loopback socket that select() does watch.)
"""

import os
import random
import select
import socket
import sys
import threading
import time

import protocol as P
import storm

ROUND_TIMEOUT = 15.0    # seconds to wait for one ring round to come back
MAX_ROUNDS = 3          # Step 2 attempts before giving up
BATCH = 300             # leader syncs the ring after this many store messages
LONG_WAIT = 600.0       # patience for rebuilds (they reload the whole dataset)
SMALL = 500             # datasets up to this size trace every store message


class Peer:
    """State and behaviour of one peer process."""

    def __init__(self, mgr_ip, mgr_port):
        self.mgr = (mgr_ip, mgr_port)
        self.name = "peer"          # trace label until registered
        self.ip = None              # our registered IPv4 address
        self.p_port = None          # our registered p-port
        self.m_sock = None          # peer<->manager socket
        self.p_sock = None          # peer<->peer socket
        self.id = None              # ring id (0 = leader); None if not in a DHT
        self.n = 0                  # ring size
        self.ring = []              # tuples (name, ip, p_port) indexed by id
        self.year = None            # dataset year of the DHT we belong to
        self.l = 0                  # number of records in the dataset
        self.s = 0                  # local hash-table size
        self.table = {}             # pos -> list of records (chaining)
        self.rid = 0                # ring-round sequence number
        self.nstore = 0             # store messages handled (for compact tracing)
        self.done = False           # set after deregister: leave the main loop

    # ---- tracing -------------------------------------------------------
    def say(self, text):
        """Print one labelled trace line."""
        print("[%s] %s" % (self.name, text), flush=True)

    def mlog(self, kind, addr, msg):
        """Trace callback for manager traffic."""
        self.say("%s %s %s:%d  %s" % (kind, "to" if kind == "SEND" else "from",
                                     addr[0], addr[1], msg))

    def trace_store(self, text):
        """Trace a store message; big datasets are summarised, not listed one by one."""
        self.nstore += 1
        if self.l <= SMALL:
            self.say(text)
        elif self.nstore <= 5 or self.nstore % 5000 == 0:
            self.say("%s  [store msgs handled so far: %d]" % (text, self.nstore))

    # ---- helpers -------------------------------------------------------
    def count(self):
        """Total records held locally (sum of chain lengths)."""
        return sum(len(v) for v in self.table.values())

    def right(self):
        """Address (ip, p_port) of the right neighbour: ring[(id+1) % n]."""
        t = self.ring[(self.id + 1) % self.n]
        return (t[1], t[2])

    def clear_dht_state(self):
        """Forget everything about the DHT (the peer is Free again)."""
        self.id, self.n, self.ring, self.table, self.s = None, 0, [], {}, 0

    def psend(self, addr, msg, trace=True):
        """Send one peer message on the p-port socket."""
        if trace:
            self.say("SEND to %s:%d  %s" % (addr[0], addr[1], self.brief(msg)))
        self.p_sock.sendto(P.encode(msg), addr)

    @staticmethod
    def brief(msg):
        """Shorten big messages for trace output (the record body of a store)."""
        return {k: v for k, v in msg.items() if k != "rec"} if msg.get("cmd") == "store" else msg

    def manager_call(self, msg):
        """Send a request on the m-port and wait (timeout + retries) for the reply."""
        if self.m_sock is None:
            self.say("not registered yet: use register first")
            return None
        reply = P.request(self.m_sock, self.mgr, msg,
                          lambda r: r.get("cmd") == msg["cmd"] and "status" in r,
                          log=self.mlog)
        if reply is None:
            self.say("no reply from manager after %d tries" % (P.RETRIES + 1))
        return reply

    def ok(self, reply):
        """True if the manager replied SUCCESS."""
        return bool(reply) and reply["status"] == P.SUCCESS

    # ---- typed commands ------------------------------------------------
    def do_register(self, a):
        """register <name> <ip> <m-port> <p-port>: bind sockets, ask manager."""
        if len(a) != 4 or not (a[2].isdigit() and a[3].isdigit()):
            return self.say("usage: register <name> <ip> <m-port> <p-port>")
        if self.m_sock is not None:
            return self.say("this process is already registered as %s" % self.name)
        name, ip, mp, pp = a[0], a[1], int(a[2]), int(a[3])
        self.name = name
        try:
            m_sock = P.make_socket(mp, "")      # m-port needed to talk to manager
        except OSError as e:
            self.name = "peer"
            return self.say("cannot bind m-port: %s" % e)
        self.m_sock = m_sock
        reply = self.manager_call({"cmd": "register", "name": name, "ip": ip,
                                   "m_port": mp, "p_port": pp})
        if self.ok(reply):
            try:                                # p-port only after manager accepts
                self.p_sock = P.make_socket(pp, "", rcvbuf=P.RCVBUF)
            except OSError as e:
                return self.say("registered but cannot bind p-port: %s" % e)
            self.ip, self.p_port = ip, pp
            self.say("registered")
        else:                                   # FAILURE: release the m-port again
            m_sock.close()
            self.m_sock = None
            self.name = "peer"

    def do_setup(self, a):
        """setup-dht <name> <n> <YYYY>: ask manager, then run leader procedure."""
        if len(a) != 3 or not (a[1].isdigit() and a[2].isdigit()):
            return self.say("usage: setup-dht <name> <n> <YYYY>")
        reply = self.manager_call({"cmd": "setup-dht", "name": a[0],
                                   "n": int(a[1]), "year": int(a[2])})
        if self.ok(reply):
            self.leader_procedure(reply["peers"], reply["year"])

    def do_complete(self, a):
        """dht-complete <name>: tell manager the DHT is built."""
        if len(a) != 1:
            return self.say("usage: dht-complete <name>")
        self.manager_call({"cmd": "dht-complete", "name": a[0]})

    def do_query(self, a):
        """query-dht <name> <event_id>: get a random DHT node, then send it find-event."""
        if len(a) != 2 or not a[1].isdigit():
            return self.say("usage: query-dht <name> <event_id>")
        reply = self.manager_call({"cmd": "query-dht", "name": a[0]})
        if not self.ok(reply):
            return
        node = reply["peer"]
        self.find_event(node, int(a[1]))

    def do_leave(self, a):
        """leave-dht <name>: teardown, renumber the ring without us, rebuild, report."""
        if len(a) != 1:
            return self.say("usage: leave-dht <name>")
        reply = self.manager_call({"cmd": "leave-dht", "name": a[0]})
        if not self.ok(reply):
            return
        n, i, ring = self.n, self.id, self.ring
        right = ring[(i + 1) % n]                       # becomes the new leader
        # 1. teardown: every node (and then we) delete the local hash table.
        if self.ring_round("teardown", {"full": False}) is None:
            return self.say("teardown round failed; aborting leave")
        self.table = {}
        # 2. reset-id: our right neighbour becomes id 0 of a ring of n-1 without us.
        new_ring = [ring[(i + 1 + k) % n] for k in range(n - 1)]
        if self.ring_round("reset-id", {"new_id": 0, "n": n - 1, "ring": new_ring}) is None:
            return self.say("reset-id round failed; aborting leave")
        # 3. rebuild-dht: the new leader reloads the data and tells us when done.
        self.psend((right[1], right[2]), {"cmd": "rebuild-dht", "year": self.year,
                                          "reply_to": [self.name, self.ip, self.p_port]})
        if self.wait_p(lambda m: m["cmd"] == "rebuild-done", LONG_WAIT) is None:
            return self.say("new leader never finished rebuilding")
        self.clear_dht_state()
        # 4. report to the manager with the new leader.
        self.manager_call({"cmd": "dht-rebuilt", "name": a[0], "new_leader": right[0]})

    def do_join(self, a):
        """join-dht <name>: ask the leader to add us to the ring, then report."""
        if len(a) != 1:
            return self.say("usage: join-dht <name>")
        reply = self.manager_call({"cmd": "join-dht", "name": a[0]})
        if not self.ok(reply):
            return
        leader = reply["leader"]
        self.psend((leader[1], leader[2]),
                   {"cmd": "join-request", "tuple": [self.name, self.ip, self.p_port]})
        # serve=True: the leader's set-id reaches us while we wait for join-done.
        if self.wait_p(lambda m: m["cmd"] == "join-done", LONG_WAIT, serve=True) is None:
            return self.say("leader never finished the join")
        self.manager_call({"cmd": "dht-rebuilt", "name": a[0], "new_leader": leader[0]})

    def do_teardown(self, a):
        """teardown-dht <name>: leader deletes the DHT around the ring, then reports."""
        if len(a) != 1:
            return self.say("usage: teardown-dht <name>")
        reply = self.manager_call({"cmd": "teardown-dht", "name": a[0]})
        if not self.ok(reply):
            return
        self.ring_round("teardown", {"full": True})
        self.clear_dht_state()
        self.manager_call({"cmd": "teardown-complete", "name": a[0]})

    def do_deregister(self, a):
        """deregister <name>: remove our state at the manager, then exit."""
        if len(a) != 1:
            return self.say("usage: deregister <name>")
        if self.ok(self.manager_call({"cmd": "deregister", "name": a[0]})):
            self.say("deregistered; exiting")
            self.done = True

    def command(self, line):
        """Dispatch one typed line."""
        w = line.split()
        if not w:
            return
        table = {"register": self.do_register, "setup-dht": self.do_setup,
                 "dht-complete": self.do_complete, "query-dht": self.do_query,
                 "leave-dht": self.do_leave, "join-dht": self.do_join,
                 "teardown-dht": self.do_teardown, "deregister": self.do_deregister}
        if w[0] in table:
            table[w[0]](w[1:])
        else:
            self.say("unknown command: %s" % w[0])

    # ---- waiting on the p-port --------------------------------------------
    def wait_p(self, match, timeout, serve=False, quiet=False):
        """Block on p_sock for a message accepted by match(); None on timeout.

        Other messages are dropped, unless serve=True: then they are handled
        normally (a joining peer must still process the leader's set-id).
        """
        end = time.time() + timeout
        while True:
            left = end - time.time()
            if left <= 0:
                return None
            r, _, _ = select.select([self.p_sock], [], [], left)
            if not r:
                return None
            try:
                data, src = self.p_sock.recvfrom(P.MAX_DGRAM)
            except ConnectionResetError:
                continue
            msg = P.decode(data)
            if not msg:
                continue
            if match(msg):
                if not quiet:
                    self.say("RECV from %s:%d  %s" % (src[0], src[1], msg))
                return msg
            if serve:
                self.on_p_msg(msg, src)

    def retry_p(self, addr, msg, match):
        """Send on the p-port, wait for matching reply; 2 s timeout, 3 retries."""
        for _ in range(P.RETRIES + 1):
            self.psend(addr, msg)
            r = self.wait_p(match, P.TIMEOUT)
            if r:
                return r
        return None

    def ring_round(self, cmd, payload):
        """Generic round: send `cmd` around the ring from this node and wait for it to return.

        Every other node applies its local action (see on_p_msg) and forwards
        to its right neighbour; the message ends back here. The same helper
        drives count, reset, sync, teardown and reset-id, so teardown is
        reusable by leave-dht and join-dht. A round id (rid) lets us ignore
        stale copies produced by a retransmission.
        """
        self.rid += 1
        msg = dict(payload, cmd=cmd, rid=self.rid)
        quiet = cmd == "sync"
        for _ in range(P.RETRIES):
            self.psend(self.right(), msg, trace=not quiet)
            r = self.wait_p(lambda m: m["cmd"] == cmd and m.get("rid") == msg["rid"],
                            ROUND_TIMEOUT, quiet=quiet)
            if r:
                return r
        return None

    # ---- leader procedure (spec 1.2.1) --------------------------------
    def leader_procedure(self, tuples, year):
        """Run Steps 1-3: assign ids, distribute records, verify, finish."""
        try:
            records = storm.load_records(year)
        except FileNotFoundError:
            self.say("details-%s.csv not found; cannot build DHT" % year)
            return
        self.ring, self.n, self.id, self.year = [tuple(t) for t in tuples], len(tuples), 0, year
        self.l = len(records)
        self.s = storm.table_size(self.l)       # first prime > 2l

        # Step 1: set-id directly to each peer i, wait for ack (2 s x 4 tries).
        for i in range(1, self.n):
            if not self.send_set_id(i):
                return self.say("peer %s never acked set-id; aborting" % self.ring[i][0])

        self.distribute(records)
        self.do_complete([self.name])

    def send_set_id(self, i):
        """Send set-id to ring[i] and wait for its ack; True if acknowledged."""
        t = self.ring[i]
        msg = {"cmd": "set-id", "id": i, "n": self.n, "year": self.year,
               "s": self.s, "l": self.l, "ring": self.ring}
        return self.retry_p((t[1], t[2]), msg,
                            lambda m: m["cmd"] == "set-id-ack" and m.get("id") == i) is not None

    def distribute(self, records):
        """Steps 2-3: hash and store every record, verify the count, print the table.

        If the total is wrong (a UDP store was lost) every node is reset and
        the records are sent again, up to MAX_ROUNDS times.
        """
        l = len(records)
        self.l, self.s = l, storm.table_size(l)
        self.say("l=%d records, s=%d, n=%d" % (l, self.s, self.n))
        self.table = {}
        counts = []
        for rnd in range(1, MAX_ROUNDS + 1):
            self.say("round %d: storing %d records" % (rnd, l))
            for k, rec in enumerate(records, 1):
                self.store_local_or_forward(rec)
                if k % BATCH == 0:
                    # Flow control: a sync can only return after every node has
                    # handled the stores sent before it (messages stay in order).
                    self.ring_round("sync", {})
                if l > SMALL and k % 5000 == 0:
                    self.say("progress: %d/%d records handled" % (k, l))
            res = self.ring_round("count", {"counts": [self.count()]})
            counts = res["counts"] if res else []
            if len(counts) == self.n and sum(counts) == l:
                break
            self.say("round %d: total %s != %d (lost store?) -> reset"
                     % (rnd, sum(counts), l))
            self.table = {}
            self.ring_round("reset", {})
        self.print_summary(counts, l)
        return counts

    def store_local_or_forward(self, rec):
        """Hash a record; keep it if it belongs to node 0, else pass it right."""
        pos, nid = storm.place(rec[0], self.s, self.n)
        if nid == self.id:
            self.table.setdefault(pos, []).append(rec)
        else:
            # Always to the RIGHT neighbour, never directly to node nid.
            self.psend(self.right(), {"cmd": "store", "id": nid, "pos": pos, "rec": rec},
                       trace=False)
            self.trace_store("SEND store event %s for id %d -> right neighbour %s:%d"
                             % ((rec[0], nid) + self.right()))

    def print_summary(self, counts, l):
        """Leader's final table: id, name, ip:port, records stored."""
        self.say("---- DHT built ----")
        self.say("%-4s %-15s %-21s %s" % ("id", "name", "ip:port", "records"))
        for i, t in enumerate(self.ring):
            c = counts[i] if i < len(counts) else "?"
            self.say("%-4d %-15s %-21s %s" % (i, t[0], "%s:%d" % (t[1], t[2]), c))
        self.say("total records: %d (expected %d)" % (sum(counts), l))

    # ---- find-event (hot potato) --------------------------------------
    def find_event(self, node, event_id):
        """Send find-event to `node` and print the answer (we are peer S)."""
        msg = {"cmd": "find-event", "event_id": event_id, "id_seq": [], "unvisited": [],
               "origin": [self.name, self.ip, self.p_port]}
        resp = self.retry_p((node[1], node[2]), msg,
                            lambda m: m["cmd"] == "find-event-resp" and m.get("event_id") == event_id)
        if resp is None:
            return self.say("no answer to find-event %d" % event_id)
        if resp["status"] != P.SUCCESS:
            return self.say("Storm event %d not found in the DHT." % event_id)
        self.say("Storm event %d found:" % event_id)
        for label, value in zip(storm.FIELDS, resp["rec"]):
            self.say("  %-18s %s" % (label + ":", value))
        self.say("  id-seq: %s" % ", ".join(str(i) for i in resp["id_seq"]))

    def on_find_event(self, msg):
        """Handle find-event: answer if we own the record, else pass the hot potato on."""
        eid = int(msg["event_id"])
        pos, owner = storm.place(eid, self.s, self.n)
        seq, unvisited = msg["id_seq"], msg["unvisited"]
        if not seq:                                    # we are the entry node
            seq = [self.id]
            unvisited = [i for i in range(self.n) if i != self.id]
        origin = (msg["origin"][1], msg["origin"][2])
        if owner == self.id:                           # the record can only be here
            rec = next((r for r in self.table.get(pos, []) if r[0] == str(eid)), None)
            ans = {"cmd": "find-event-resp", "event_id": eid, "id_seq": seq,
                   "status": P.SUCCESS if rec else P.FAILURE, "rec": rec}
            self.say("find-event %d: I own pos %d -> %s" % (eid, pos, "found" if rec else "not found"))
            return self.psend(origin, ans)
        if not unvisited:                              # everyone was tried
            return self.psend(origin, {"cmd": "find-event-resp", "event_id": eid,
                                       "id_seq": seq, "status": P.FAILURE, "rec": None})
        nxt = random.choice(unvisited)                 # hot potato: random unvisited node
        unvisited.remove(nxt)
        seq.append(nxt)
        t = self.ring[nxt]
        self.say("find-event %d: owner is id %d, not me -> hot potato to id %d" % (eid, owner, nxt))
        self.psend((t[1], t[2]), dict(msg, id_seq=seq, unvisited=unvisited))

    # ---- peer message handler (all peers) -----------------------------
    def on_p_msg(self, msg, src):
        """Handle one datagram arriving on the p-port."""
        cmd = msg["cmd"]
        if cmd == "set-id":
            self.id, self.n, self.s = msg["id"], msg["n"], msg["s"]
            self.year, self.l = msg["year"], msg.get("l", 0)
            self.ring = [tuple(t) for t in msg["ring"]]
            self.table = {}
            self.say("RECV set-id from %s:%d: id=%d n=%d right=%s"
                     % (src[0], src[1], self.id, self.n, self.ring[(self.id + 1) % self.n][0]))
            self.psend(src, {"cmd": "set-id-ack", "id": self.id})
        elif cmd == "store":
            if self.n == 0:
                return                                  # not in a DHT any more
            if msg["id"] == self.id:
                self.table.setdefault(msg["pos"], []).append(msg["rec"])
                self.trace_store("RECV store event %s for id %d -> stored at pos %d"
                                 % (msg["rec"][0], msg["id"], msg["pos"]))
            else:
                self.trace_store("RECV store event %s for id %d -> forwarded right"
                                 % (msg["rec"][0], msg["id"]))
                self.psend(self.right(), msg, trace=False)
        elif cmd in ("count", "reset", "sync", "teardown", "reset-id"):
            self.on_round(msg, src)
        elif cmd == "find-event":
            self.say("RECV find-event from %s:%d  %s" % (src[0], src[1], msg))
            self.on_find_event(msg)
        elif cmd == "rebuild-dht":
            self.on_rebuild(msg, src)
        elif cmd == "join-request":
            self.on_join_request(msg, src)
        else:
            self.say("RECV unexpected %s from %s:%d" % (cmd, src[0], src[1]))

    def on_round(self, msg, src):
        """Apply a ring-round message locally and forward it to the right neighbour."""
        cmd = msg["cmd"]
        addr = self.right()            # OLD right neighbour, taken before any change
        if cmd == "count":
            msg["counts"].append(self.count())          # append local count
        elif cmd == "reset":
            self.table = {}                              # empty local table
        elif cmd == "teardown":
            self.table = {}                              # delete local hash table
            if msg.get("full"):
                self.clear_dht_state()                   # full teardown: we are Free again
        elif cmd == "reset-id":
            self.id, self.n = msg["new_id"], msg["n"]    # renumber, then pass id+1 on
            self.ring = [tuple(t) for t in msg["ring"]]
            msg["new_id"] += 1
            self.say("RECV reset-id: now id=%d, ring size %d" % (self.id, self.n))
        if cmd != "sync":
            self.say("RECV %s from %s:%d -> forwarded right" % (cmd, src[0], src[1]))
        self.psend(addr, msg, trace=cmd != "sync")

    def on_rebuild(self, msg, src):
        """New leader after a leave-dht: reload the data into the smaller ring."""
        self.say("RECV rebuild-dht from %s:%d: I am the new leader (id %d)"
                 % (src[0], src[1], self.id))
        self.year = msg["year"]
        self.distribute(storm.load_records(self.year))
        who = msg["reply_to"]
        self.psend((who[1], who[2]), {"cmd": "rebuild-done"})

    def on_join_request(self, msg, src):
        """Leader: add a new peer as the last id, renumber nothing, reload the data."""
        if self.id != 0:
            return self.say("join-request ignored: I am not the leader")
        newcomer = tuple(msg["tuple"])
        self.say("RECV join-request from %s:%d: %s wants to join" % (src[0], src[1], newcomer[0]))
        records = storm.load_records(self.year)
        if self.ring_round("teardown", {"full": False}) is None:
            return self.say("teardown round failed; join aborted")
        self.table = {}
        new_ring = self.ring + [newcomer]               # newcomer gets id n, right = leader
        # Existing ids keep their numbers; reset-id just hands everyone the bigger ring.
        if self.ring_round("reset-id", {"new_id": 1, "n": self.n + 1, "ring": new_ring}) is None:
            return self.say("reset-id round failed; join aborted")
        self.ring, self.n = new_ring, self.n + 1
        if not self.send_set_id(self.n - 1):
            return self.say("newcomer never acked set-id; join aborted")
        self.distribute(records)
        self.psend((newcomer[1], newcomer[2]), {"cmd": "join-done"})

    # ---- main loop -----------------------------------------------------
    def run(self):
        """select() over [stdin, p_sock]; process typed commands and ring traffic."""
        src_in, proxy = stdin_source()
        stdin_open = True
        while not self.done:
            socks = ([src_in] if stdin_open else []) + ([self.p_sock] if self.p_sock else [])
            if not socks:
                return
            ready, _, _ = select.select(socks, [], [], 0.5)
            for r in ready:
                if r is self.p_sock:
                    try:
                        data, src = r.recvfrom(P.MAX_DGRAM)
                    except ConnectionResetError:
                        continue
                    msg = P.decode(data)
                    if msg:
                        self.on_p_msg(msg, src)
                else:
                    line = read_line(r, proxy)
                    if line is None:
                        stdin_open = False          # EOF: keep serving the ring
                    else:
                        self.command(line)
                if self.done:
                    return


def stdin_source():
    """Return (selectable, proxy_flag). On Windows relay stdin through a socket."""
    if os.name != "nt":
        return sys.stdin, False
    rx = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    rx.bind(("127.0.0.1", 0))
    dest = rx.getsockname()

    def pump():
        """Helper thread: forward each stdin line (and EOF) to rx."""
        tx = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        for line in sys.stdin:
            tx.sendto(line.encode("utf-8"), dest)
        tx.sendto(b"\x00EOF", dest)
    threading.Thread(target=pump, daemon=True).start()
    return rx, True


def read_line(src, proxy):
    """Read one command line from the stdin source; None on EOF."""
    if proxy:
        data = src.recvfrom(65535)[0]
        return None if data == b"\x00EOF" else data.decode("utf-8")
    line = sys.stdin.readline()
    return line if line else None


def main():
    """Parse arguments and start the peer."""
    if len(sys.argv) != 3:
        sys.exit("usage: python peer.py <manager-IPv4> <manager-port>")
    Peer(sys.argv[1], int(sys.argv[2])).run()


if __name__ == "__main__":
    main()
