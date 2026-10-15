"""manager.py - always-on DHT manager (UDP).

Usage:
    python manager.py <port>

Role in the protocol:
    Keeps the peer state table (Free | Leader | InDHT), validates every
    manager command (register, setup-dht, dht-complete, query-dht, leave-dht,
    join-dht, dht-rebuilt, deregister, teardown-dht, teardown-complete) and
    answers each with exactly one SUCCESS or FAILURE datagram.

    Some commands start a multi-step job that only the requesting peer can
    finish (dht-complete, dht-rebuilt, teardown-complete). While one is
    running the manager is "blocked": `awaiting` holds the command it needs
    and every other message gets FAILURE.
"""

import random
import sys

import protocol as P

FREE, LEADER, INDHT = "Free", "Leader", "InDHT"
MAX_PEERS = 64          # upper bound on registered peers


class Manager:
    """Holds manager state and implements one handler per command."""

    def __init__(self):
        self.peers = {}         # name -> {ip, m_port, p_port, state}
        self.leader = None      # name of the current leader (None if no DHT)
        self.dht_exists = False
        self.members = []       # names of the peers maintaining the DHT
        self.awaiting = None    # command the manager is blocked on, if any
        self.initiator = None   # peer that started the job we are waiting on
        self.pending = None     # "leave" or "join" while awaiting dht-rebuilt

    # ---- helpers -------------------------------------------------------
    def used_ports(self):
        """Return the set of all ports already claimed by registered peers."""
        return {p for v in self.peers.values() for p in (v["m_port"], v["p_port"])}

    def tuple_of(self, name):
        """The (name, ip, p_port) 3-tuple the protocol uses to address a peer."""
        v = self.peers[name]
        return [name, v["ip"], v["p_port"]]

    def print_table(self):
        """Print the state table (called after every SUCCESS)."""
        print("[manager] state table:  leader=%s dht_exists=%s awaiting=%s"
              % (self.leader, self.dht_exists, self.awaiting))
        for n, v in self.peers.items():
            print("[manager]   %-15s %-15s m=%-5d p=%-5d %s"
                  % (n, v["ip"], v["m_port"], v["p_port"], v["state"]))

    @staticmethod
    def _valid_ip(ip):
        """True if ip is dotted-quad IPv4."""
        parts = str(ip).split(".")
        return len(parts) == 4 and all(p.isdigit() and int(p) < 256 for p in parts)

    def _wait_for(self, cmd, name, pending=None):
        """Enter the blocked state: only `cmd` from `name` is accepted next."""
        self.awaiting, self.initiator, self.pending = cmd, name, pending

    def _done_waiting(self):
        """Leave the blocked state and resume normal processing."""
        self.awaiting = self.initiator = self.pending = None

    def _refresh_states(self):
        """Members are InDHT, except the leader who is Leader."""
        for m in self.members:
            self.peers[m]["state"] = LEADER if m == self.leader else INDHT

    # ---- milestone commands ---------------------------------------------
    def register(self, m):
        """Validate and add a peer as Free; FAILURE changes nothing."""
        cmd = "register"
        try:
            name, ip = m["name"], m["ip"]
            mp, pp = int(m["m_port"]), int(m["p_port"])
        except (KeyError, ValueError, TypeError):
            return P.response(cmd, P.FAILURE, "malformed request")
        if not (isinstance(name, str) and name.isascii() and name.isalpha()
                and len(name) <= 15):
            return P.response(cmd, P.FAILURE, "name must be alphabetic, <= 15 chars")
        if name in self.peers:
            return P.response(cmd, P.FAILURE, "name already registered")
        if len(self.peers) >= MAX_PEERS:
            return P.response(cmd, P.FAILURE, "too many peers")
        if not self._valid_ip(ip):
            return P.response(cmd, P.FAILURE, "bad IPv4 address")
        if mp == pp:
            return P.response(cmd, P.FAILURE, "m-port must differ from p-port")
        if mp in self.used_ports() or pp in self.used_ports():
            return P.response(cmd, P.FAILURE, "port already in use")
        self.peers[name] = {"ip": ip, "m_port": mp, "p_port": pp, "state": FREE}
        return P.response(cmd, P.SUCCESS)

    def setup_dht(self, m):
        """Pick leader + n-1 random Free peers; reply with n tuples, leader first."""
        cmd = "setup-dht"
        try:
            name, n, year = m["name"], int(m["n"]), int(m["year"])
        except (KeyError, ValueError, TypeError):
            return P.response(cmd, P.FAILURE, "malformed request")
        if name not in self.peers:
            return P.response(cmd, P.FAILURE, "peer not registered")
        if n < 3:
            return P.response(cmd, P.FAILURE, "n must be >= 3")
        if len(self.peers) < n:
            return P.response(cmd, P.FAILURE, "fewer than n peers registered")
        if self.dht_exists:
            return P.response(cmd, P.FAILURE, "a DHT already exists")
        if self.peers[name]["state"] != FREE:
            return P.response(cmd, P.FAILURE, "peer is not Free")
        free = [k for k, v in self.peers.items() if v["state"] == FREE and k != name]
        if len(free) < n - 1:
            return P.response(cmd, P.FAILURE, "not enough Free peers")
        chosen = random.sample(free, n - 1)          # random selection
        self.leader, self.dht_exists = name, True
        self.members = [name] + chosen               # leader first
        self._refresh_states()
        self._wait_for("dht-complete", name)
        tuples = [self.tuple_of(k) for k in self.members]
        return P.response(cmd, P.SUCCESS, peers=tuples, n=n, year=year)

    def dht_complete(self, m):
        """Leader-only: release the manager from its waiting state."""
        cmd = "dht-complete"
        if m.get("name") != self.leader or self.awaiting != cmd:
            return P.response(cmd, P.FAILURE, "sender is not the leader")
        self._done_waiting()
        return P.response(cmd, P.SUCCESS)

    # ---- full-project commands --------------------------------------------
    def query_dht(self, m):
        """Return the tuple of a random DHT node to a registered, Free peer."""
        cmd, name = "query-dht", m.get("name")
        if name not in self.peers:
            return P.response(cmd, P.FAILURE, "peer not registered")
        if not self.dht_exists:
            return P.response(cmd, P.FAILURE, "DHT does not exist")
        if self.peers[name]["state"] != FREE:
            return P.response(cmd, P.FAILURE, "peer is not Free")
        return P.response(cmd, P.SUCCESS, peer=self.tuple_of(random.choice(self.members)))

    def leave_dht(self, m):
        """A DHT member asks to leave; wait for it to send dht-rebuilt."""
        cmd, name = "leave-dht", m.get("name")
        if name not in self.peers:
            return P.response(cmd, P.FAILURE, "peer not registered")
        if not self.dht_exists:
            return P.response(cmd, P.FAILURE, "DHT does not exist")
        if name not in self.members:
            return P.response(cmd, P.FAILURE, "peer is not in the DHT")
        self._wait_for("dht-rebuilt", name, "leave")
        return P.response(cmd, P.SUCCESS)

    def join_dht(self, m):
        """A Free peer asks to join; reply with the leader's tuple to contact."""
        cmd, name = "join-dht", m.get("name")
        if name not in self.peers:
            return P.response(cmd, P.FAILURE, "peer not registered")
        if not self.dht_exists:
            return P.response(cmd, P.FAILURE, "DHT does not exist")
        if self.peers[name]["state"] != FREE:
            return P.response(cmd, P.FAILURE, "peer is not Free")
        self._wait_for("dht-rebuilt", name, "join")
        return P.response(cmd, P.SUCCESS, leader=self.tuple_of(self.leader))

    def dht_rebuilt(self, m):
        """Initiator reports the ring was rebuilt: update members, states, leader."""
        cmd, name, new_leader = "dht-rebuilt", m.get("name"), m.get("new_leader")
        if name != self.initiator or self.awaiting != cmd:
            return P.response(cmd, P.FAILURE, "sender did not start the leave/join")
        if self.pending == "leave":
            self.members.remove(name)
            self.peers[name]["state"] = FREE
        else:
            self.members.append(name)
        if new_leader not in self.members:
            if self.pending == "leave":          # undo is not possible; keep going
                pass
            return P.response(cmd, P.FAILURE, "new-leader is not in the DHT")
        self.leader = new_leader
        self._refresh_states()
        self._done_waiting()
        return P.response(cmd, P.SUCCESS)

    def deregister(self, m):
        """Remove a Free peer's state so its process can exit."""
        cmd, name = "deregister", m.get("name")
        if name not in self.peers:
            return P.response(cmd, P.FAILURE, "peer not registered")
        if self.peers[name]["state"] != FREE:
            return P.response(cmd, P.FAILURE, "peer is in the DHT")
        del self.peers[name]
        return P.response(cmd, P.SUCCESS)

    def teardown_dht(self, m):
        """Leader asks to delete the DHT; wait for teardown-complete."""
        cmd = "teardown-dht"
        if not self.dht_exists or m.get("name") != self.leader:
            return P.response(cmd, P.FAILURE, "sender is not the leader")
        self._wait_for("teardown-complete", self.leader)
        return P.response(cmd, P.SUCCESS)

    def teardown_complete(self, m):
        """Leader confirms the DHT is gone: every member becomes Free."""
        cmd = "teardown-complete"
        if m.get("name") != self.leader or self.awaiting != cmd:
            return P.response(cmd, P.FAILURE, "sender is not the leader")
        for name in self.members:
            self.peers[name]["state"] = FREE
        self.members, self.leader, self.dht_exists = [], None, False
        self._done_waiting()
        return P.response(cmd, P.SUCCESS)

    # ---- dispatcher ----------------------------------------------------
    def handle(self, m):
        """Route one decoded request and return the single reply dict."""
        cmd = m.get("cmd")
        if self.awaiting and cmd != self.awaiting:
            # Blocked: only the awaited command is acceptable.
            return P.response(str(cmd), P.FAILURE,
                              "manager is waiting for %s" % self.awaiting)
        handlers = {"register": self.register, "setup-dht": self.setup_dht,
                    "dht-complete": self.dht_complete, "query-dht": self.query_dht,
                    "leave-dht": self.leave_dht, "join-dht": self.join_dht,
                    "dht-rebuilt": self.dht_rebuilt, "deregister": self.deregister,
                    "teardown-dht": self.teardown_dht,
                    "teardown-complete": self.teardown_complete}
        if cmd not in handlers:
            return P.response(str(cmd), P.FAILURE, "unknown command")
        return handlers[cmd](m)


def main():
    """Bind the UDP socket and serve requests forever."""
    if len(sys.argv) != 2:
        sys.exit("usage: python manager.py <port>")
    port = int(sys.argv[1])
    sock = P.make_socket(port, "")
    mgr = Manager()
    print("[manager] listening on UDP port %d" % port, flush=True)
    while True:
        data, src = sock.recvfrom(P.MAX_DGRAM)
        msg = P.decode(data)
        print("[manager] RECV from %s:%d  %s" % (src[0], src[1], msg or data), flush=True)
        reply = mgr.handle(msg) if msg else P.response("?", P.FAILURE, "malformed message")
        sock.sendto(P.encode(reply), src)
        print("[manager] SEND to %s:%d  %s" % (src[0], src[1], reply), flush=True)
        if reply["status"] == P.SUCCESS:
            mgr.print_table()
            sys.stdout.flush()


if __name__ == "__main__":
    main()
