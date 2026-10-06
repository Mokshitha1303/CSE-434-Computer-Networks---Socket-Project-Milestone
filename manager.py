"""manager.py - always-on DHT manager (UDP).

Usage:
    python manager.py <port>

Role in the protocol:
    Keeps the peer state table (Free | Leader | InDHT), validates register /
    setup-dht / dht-complete requests and answers each with exactly one
    SUCCESS or FAILURE datagram. After a successful setup-dht it is blocked
    on `awaiting == "dht-complete"` and rejects everything else.
"""

import random
import sys

import protocol as P

FREE, LEADER, INDHT = "Free", "Leader", "InDHT"
MAX_PEERS = 64          # upper bound on registered peers


class Manager:
    """Holds manager state and implements one handler per command."""

    def __init__(self):
        self.peers = {}        # name -> {ip, m_port, p_port, state}
        self.leader = None     # name of current leader (None if no DHT)
        self.dht_exists = False
        self.awaiting = None   # command the manager is blocked on, if any

    # ---- helpers -------------------------------------------------------
    def used_ports(self):
        """Return the set of all ports already claimed by registered peers."""
        return {p for v in self.peers.values() for p in (v["m_port"], v["p_port"])}

    def print_table(self):
        """Print the state table (called after every SUCCESS)."""
        print("[manager] state table:  leader=%s dht_exists=%s awaiting=%s"
              % (self.leader, self.dht_exists, self.awaiting))
        for n, v in self.peers.items():
            print("[manager]   %-15s %-15s m=%-5d p=%-5d %s"
                  % (n, v["ip"], v["m_port"], v["p_port"], v["state"]))

    # ---- command handlers ---------------------------------------------
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

    @staticmethod
    def _valid_ip(ip):
        """True if ip is dotted-quad IPv4."""
        parts = str(ip).split(".")
        return (len(parts) == 4 and all(p.isdigit() and int(p) < 256 for p in parts))

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
        self.peers[name]["state"] = LEADER
        for c in chosen:
            self.peers[c]["state"] = INDHT
        self.leader, self.dht_exists, self.awaiting = name, True, "dht-complete"
        tuples = [[k, self.peers[k]["ip"], self.peers[k]["p_port"]]
                  for k in [name] + chosen]           # leader's tuple first
        return P.response(cmd, P.SUCCESS, peers=tuples, n=n, year=year)

    def dht_complete(self, m):
        """Leader-only: release the manager from its waiting state."""
        cmd = "dht-complete"
        if m.get("name") != self.leader or self.awaiting != "dht-complete":
            return P.response(cmd, P.FAILURE, "sender is not the leader")
        self.awaiting = None
        return P.response(cmd, P.SUCCESS)

    # ---- dispatcher ----------------------------------------------------
    def handle(self, m):
        """Route one decoded request and return the single reply dict."""
        cmd = m.get("cmd")
        if self.awaiting and cmd != self.awaiting:
            # Blocked: only dht-complete is acceptable.
            return P.response(str(cmd), P.FAILURE,
                              "manager is waiting for %s" % self.awaiting)
        handlers = {"register": self.register, "setup-dht": self.setup_dht,
                    "dht-complete": self.dht_complete}
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
