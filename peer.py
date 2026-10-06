"""peer.py - DHT peer (UDP, single thread, select()).

Usage:
    python peer.py <manager-IPv4> <manager-port>

Typed commands (stdin):
    register <name> <ip> <m-port> <p-port>
    setup-dht <name> <n> <YYYY>
    dht-complete <name>          (the leader also sends this automatically)

Role in the protocol:
    * m-port socket: strict request/response with the manager.
    * p-port socket: ring traffic (set-id, store, count, reset). Messages
      always travel one way around the ring: 0 -> 1 -> ... -> n-1 -> 0.
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

import protocol as P
import storm

ROUND_TIMEOUT = 5.0     # seconds to wait for a ring round to return
MAX_ROUNDS = 3          # Step 2 attempts before giving up


class Peer:
    """State and behaviour of one peer process."""

    def __init__(self, mgr_ip, mgr_port):
        self.mgr = (mgr_ip, mgr_port)
        self.name = "peer"          # trace label until registered
        self.m_sock = None          # peer<->manager socket
        self.p_sock = None          # peer<->peer socket
        self.id = None              # ring id (0 = leader)
        self.n = 0                  # ring size
        self.ring = []              # tuples (name, ip, p_port) indexed by id
        self.s = 0                  # local hash-table size
        self.table = {}             # pos -> list of records (chaining)
        self.rid = 0                # ring-round sequence number

    # ---- tracing -------------------------------------------------------
    def say(self, text):
        """Print one labelled trace line."""
        print("[%s] %s" % (self.name, text), flush=True)

    def mlog(self, kind, addr, msg):
        """Trace callback for manager traffic."""
        self.say("%s %s %s:%d  %s" % (kind, "to" if kind == "SEND" else "from",
                                     addr[0], addr[1], msg))

    # ---- helpers -------------------------------------------------------
    def count(self):
        """Total records held locally (sum of chain lengths)."""
        return sum(len(v) for v in self.table.values())

    def right(self):
        """Address (ip, p_port) of the right neighbour: ring[(id+1) % n]."""
        t = self.ring[(self.id + 1) % self.n]
        return (t[1], t[2])

    def psend(self, addr, msg, trace=True):
        """Send one ring message on the p-port socket."""
        if trace:
            self.say("SEND to %s:%d  %s" % (addr[0], addr[1], self.brief(msg)))
        self.p_sock.sendto(P.encode(msg), addr)

    @staticmethod
    def brief(msg):
        """Shorten big messages (e.g. set-id's ring) for trace output."""
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

    # ---- typed commands ------------------------------------------------
    def do_register(self, a):
        """register <name> <ip> <m-port> <p-port>: bind both sockets, ask manager."""
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
        if reply and reply["status"] == P.SUCCESS:
            try:                                # p-port only after manager accepts
                self.p_sock = P.make_socket(pp, "", rcvbuf=P.RCVBUF)
            except OSError as e:
                return self.say("registered but cannot bind p-port: %s" % e)
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
        if reply and reply["status"] == P.SUCCESS:
            self.leader_procedure(reply["peers"], reply["year"])

    def do_complete(self, a):
        """dht-complete <name>: tell manager the DHT is built."""
        if len(a) != 1:
            return self.say("usage: dht-complete <name>")
        self.manager_call({"cmd": "dht-complete", "name": a[0]})

    def command(self, line):
        """Dispatch one typed line."""
        w = line.split()
        if not w:
            return
        table = {"register": self.do_register, "setup-dht": self.do_setup,
                 "dht-complete": self.do_complete}
        if w[0] in table:
            table[w[0]](w[1:])
        else:
            self.say("unknown command: %s" % w[0])

    # ---- leader procedure (spec 1.2.1) --------------------------------
    def wait_p(self, match, timeout):
        """Block on p_sock for a message accepted by match(); None on timeout."""
        import time
        end = time.time() + timeout
        while True:
            left = end - time.time()
            if left <= 0:
                return None
            r, _, _ = select.select([self.p_sock], [], [], left)
            if not r:
                return None
            data, src = self.p_sock.recvfrom(P.MAX_DGRAM)
            msg = P.decode(data)
            if msg and match(msg):
                self.say("RECV from %s:%d  %s" % (src[0], src[1], msg))
                return msg

    def leader_procedure(self, tuples, year):
        """Run Steps 1-3: assign ids, distribute records, verify, finish."""
        try:
            records = storm.load_records(year)
        except FileNotFoundError:
            self.say("details-%s.csv not found; cannot build DHT" % year)
            return
        self.ring, self.n, self.id = [tuple(t) for t in tuples], len(tuples), 0
        l = len(records)
        self.s = storm.table_size(l)       # first prime > 2l
        self.say("l=%d records, s=%d, n=%d" % (l, self.s, self.n))

        # Step 1: set-id directly to each peer i, wait for ack (2 s x 4 tries).
        for i in range(1, self.n):
            t = self.ring[i]
            msg = {"cmd": "set-id", "id": i, "n": self.n, "year": year,
                   "s": self.s, "ring": self.ring}
            ack = self.retry_p((t[1], t[2]), msg,
                               lambda m, i=i: m["cmd"] == "set-id-ack" and m.get("id") == i)
            if ack is None:
                return self.say("peer %s never acked set-id; aborting" % t[0])

        # Step 2 + 3: store, count, reset-and-retry if the total is wrong.
        counts = []
        for rnd in range(1, MAX_ROUNDS + 1):
            self.say("round %d: storing %d records" % (rnd, l))
            for rec in records:
                self.store_local_or_forward(rec)
            res = self.ring_round("count", {"counts": [self.count()]})
            counts = res["counts"] if res else []
            if len(counts) == self.n and sum(counts) == l:
                break
            self.say("round %d: total %s != %d (lost store?) -> reset"
                     % (rnd, sum(counts), l))
            self.table = {}
            self.ring_round("reset", {})
        self.print_summary(counts, l)
        self.do_complete([self.name])

    def retry_p(self, addr, msg, match):
        """Send on the p-port, wait for matching reply; 2 s timeout, 3 retries."""
        for _ in range(P.RETRIES + 1):
            self.psend(addr, msg)
            r = self.wait_p(match, P.TIMEOUT)
            if r:
                return r
        return None

    def ring_round(self, cmd, payload):
        """Generic leader-started round: send around the ring, wait for it to return.

        Every other node applies its local action (see on_ring_msg) and
        forwards to its right neighbour; the leader gets the message back.
        Reusable for teardown / reset-id in the full project.
        """
        self.rid += 1
        msg = dict(payload, cmd=cmd, rid=self.rid)
        # rid lets us ignore stale copies produced by a retransmission.
        for _ in range(P.RETRIES):
            self.psend(self.right(), msg)
            r = self.wait_p(lambda m: m["cmd"] == msg["cmd"] and m.get("rid") == msg["rid"],
                            ROUND_TIMEOUT)
            if r:
                return r
        return None

    def store_local_or_forward(self, rec):
        """Hash a record; keep it if it belongs to node 0, else pass it right."""
        pos, nid = storm.place(rec[0], self.s, self.n)
        if nid == self.id:
            self.table.setdefault(pos, []).append(rec)
        else:
            # Always to the RIGHT neighbour, never directly to node nid.
            self.psend(self.right(), {"cmd": "store", "id": nid, "pos": pos, "rec": rec},
                       trace=False)
            self.say("SEND store event %s for id %d -> right neighbour %s:%d"
                     % ((rec[0], nid) + self.right()))

    def print_summary(self, counts, l):
        """Leader's final table: id, name, ip:port, records stored."""
        self.say("---- DHT built ----")
        self.say("%-4s %-15s %-21s %s" % ("id", "name", "ip:port", "records"))
        for i, t in enumerate(self.ring):
            c = counts[i] if i < len(counts) else "?"
            self.say("%-4d %-15s %-21s %s" % (i, t[0], "%s:%d" % (t[1], t[2]), c))
        self.say("total records: %d (expected %d)" % (sum(counts), l))

    # ---- ring traffic handler (all peers) -----------------------------
    def on_p_msg(self, msg, src):
        """Handle one datagram arriving on the p-port."""
        cmd = msg["cmd"]
        if cmd == "set-id":
            self.id, self.n, self.s = msg["id"], msg["n"], msg["s"]
            self.ring = [tuple(t) for t in msg["ring"]]
            self.table = {}
            self.say("RECV set-id from %s:%d: id=%d n=%d right=%s"
                     % (src[0], src[1], self.id, self.n,
                        self.ring[(self.id + 1) % self.n][0]))
            self.psend(src, {"cmd": "set-id-ack", "id": self.id})
        elif cmd == "store":
            if msg["id"] == self.id:
                self.table.setdefault(msg["pos"], []).append(msg["rec"])
                self.say("RECV store event %s for id %d -> stored at pos %d"
                         % (msg["rec"][0], msg["id"], msg["pos"]))
            else:
                self.say("RECV store event %s for id %d -> forwarded right"
                         % (msg["rec"][0], msg["id"]))
                self.psend(self.right(), msg, trace=False)
        elif cmd in ("count", "reset"):
            if cmd == "count":
                msg["counts"].append(self.count())     # append local count
            else:
                self.table = {}                         # empty local table
            self.say("RECV %s from %s:%d -> forwarded right" % (cmd, src[0], src[1]))
            self.psend(self.right(), msg)
        else:
            self.say("RECV unexpected %s from %s:%d" % (cmd, src[0], src[1]))

    # ---- main loop -----------------------------------------------------
    def run(self):
        """select() over [stdin, p_sock]; process typed commands and ring traffic."""
        src_in, proxy = stdin_source()
        stdin_open = True
        while True:
            socks = ([src_in] if stdin_open else []) + ([self.p_sock] if self.p_sock else [])
            if not socks:
                return
            ready, _, _ = select.select(socks, [], [], 0.5)
            for r in ready:
                if r is self.p_sock:
                    data, src = r.recvfrom(P.MAX_DGRAM)
                    msg = P.decode(data)
                    if msg:
                        self.on_p_msg(msg, src)
                else:
                    line = read_line(r, proxy)
                    if line is None:
                        stdin_open = False          # EOF: keep serving the ring
                    else:
                        self.command(line)
            if not stdin_open and not self.p_sock:
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
