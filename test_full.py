"""test_full.py - end-to-end test of the full DHT project (manager + 6 peers).

Usage:
    python test_full.py [YYYY]

Runs the demo sequence from the project spec with subprocesses and saves each
process's output to logs/full_<name>.log:
    register 6 peers -> setup-dht n=5 -> query-dht (4 event ids) -> leave-dht
    -> query by the peer that left -> join-dht -> query -> teardown-dht
    -> deregister everyone.
Uses details-1996.csv and the spec's four event ids when that file exists,
otherwise details-1950.csv. Peers use 127.0.0.1 - 127.0.0.4 to look like hosts.
"""

import os
import re
import subprocess
import sys
import time

import storm

HERE = os.path.dirname(os.path.abspath(__file__))
LOGS = os.path.join(HERE, "logs")
MGR_PORT = 1500
NAMES = ["Alice", "Bob", "Charlie", "David", "Eve", "Frank"]
HOSTS = ["127.0.0.1", "127.0.0.2", "127.0.0.3", "127.0.0.4", "127.0.0.1", "127.0.0.2"]
procs, files, results = {}, {}, []
WAIT = 900                                  # generous: big datasets take a while


def logpath(name):
    """Log file for one process."""
    return os.path.join(LOGS, "full_%s.log" % name)


def start(name, args):
    """Launch a python subprocess whose output goes to its log file."""
    f = open(logpath(name), "w", encoding="utf-8")
    procs[name] = subprocess.Popen([sys.executable, "-u"] + args, cwd=HERE,
                                   stdin=subprocess.PIPE, stdout=f,
                                   stderr=subprocess.STDOUT, text=True)
    files[name] = f


def read(name):
    """Current contents of a process log."""
    with open(logpath(name), encoding="utf-8") as f:
        return f.read()


def send(name, line):
    """Type one command into a process."""
    procs[name].stdin.write(line + "\n")
    procs[name].stdin.flush()


def wait_count(name, text, n, timeout=WAIT):
    """Wait until `text` appears at least n times in a log; True on success."""
    end = time.time() + timeout
    while time.time() < end:
        if read(name).count(text) >= n:
            return True
        time.sleep(0.3)
    return False


def last_status(name):
    """Status of the latest manager reply seen in a peer's log."""
    m = re.findall(r"RECV from [\d.]+:%d  .*?'status': '(\w+)'" % MGR_PORT, read(name))
    return m[-1] if m else None


def check(label, ok):
    """Print and record one verification result."""
    print(("PASS  " if ok else "FAIL  ") + label, flush=True)
    results.append(bool(ok))


def command(name, line, expect):
    """Send a command to a peer, wait for the manager's reply, check its status."""
    before = len(re.findall(r"RECV from [\d.]+:%d" % MGR_PORT, read(name)))
    send(name, line)
    end = time.time() + 30
    while time.time() < end:
        if len(re.findall(r"RECV from [\d.]+:%d" % MGR_PORT, read(name))) > before:
            break
        time.sleep(0.2)
    time.sleep(0.3)
    check("%s -> %s" % (line, expect), last_status(name) == expect)


def last_table():
    """Parse the last state table printed by the manager: {name: state}, header."""
    text = read("manager")
    start = text.rfind("[manager] state table:")
    block = text[start:].splitlines()
    header = block[0]
    states = {}
    for ln in block[1:]:
        m = re.match(r"\[manager\]\s+(\w+)\s+[\d.]+\s+m=\d+\s+p=\d+\s+(\w+)\s*$", ln)
        if not m:
            break
        states[m.group(1)] = m.group(2)
    return header, states


def summaries():
    """All (total, expected) pairs printed by any leader, in no particular order."""
    out = []
    for n in NAMES:
        out += re.findall(r"total records: (\d+) \(expected (\d+)\)", read(n))
    return out


def query(peer, eid, present):
    """Have `peer` run query-dht for eid and check the printed result."""
    before_found = read(peer).count("Storm event %s found:" % eid)
    before_miss = read(peer).count("Storm event %s not found" % eid)
    send(peer, "query-dht %s %s" % (peer, eid))
    key, base = ("Storm event %s found:" % eid, before_found) if present else \
                ("Storm event %s not found" % eid, before_miss)
    ok = wait_count(peer, key, base + 1, 60)
    check("query %s by %s -> %s" % (eid, peer, "found" if present else "not found"), ok)
    if present and ok:
        time.sleep(0.5)
        check("   id-seq printed", "id-seq:" in read(peer))


def main():
    """Run the whole scenario."""
    year = int(sys.argv[1]) if len(sys.argv) > 1 else \
        (1996 if os.path.exists(os.path.join(HERE, "details-1996.csv")) else 1950)
    recs = storm.load_records(year)
    have = {r[0] for r in recs}
    if year == 1996:
        ids = ["5536849", "2402920", "5539287", "55770111"]
    else:
        ids = [recs[0][0], recs[len(recs) // 2][0], recs[-1][0], "55770111"]
    print("dataset %d: %d records; query ids %s" % (year, len(recs), ids), flush=True)

    os.makedirs(LOGS, exist_ok=True)
    start("manager", ["manager.py", str(MGR_PORT)])
    time.sleep(1)
    for n in NAMES:
        start(n, ["peer.py", "127.0.0.1", str(MGR_PORT)])
    time.sleep(1)

    for i, n in enumerate(NAMES):
        command(n, "register %s %s %d %d" % (n, HOSTS[i], 1501 + 2 * i, 1502 + 2 * i), "SUCCESS")
    command("Bob", "setup-dht Bob 7 %d" % year, "FAILURE")          # too few peers
    command("Bob", "query-dht Bob 1", "FAILURE")                   # no DHT yet
    command("Bob", "join-dht Bob", "FAILURE")                      # no DHT yet
    command("Bob", "leave-dht Bob", "FAILURE")                     # no DHT yet
    command("Bob", "teardown-dht Bob", "FAILURE")                  # not leader

    # --- setup-dht n=5 ---------------------------------------------------
    send("Alice", "setup-dht Alice 5 %d" % year)
    check("setup-dht Alice 5 %d -> dht-complete SUCCESS" % year,
          wait_count("manager", "'cmd': 'dht-complete', 'status': 'SUCCESS'", 1))
    time.sleep(1)
    rows = re.findall(r"^\[Alice\] (\d)\s+(\w+)\s+[\d.]+:\d+\s+(\d+)\s*$", read("Alice"), re.M)
    ring = {int(r[0]): r[1] for r in rows}
    counts = [int(r[2]) for r in rows]
    check("5 nodes, counts sum to l (%s)" % counts, len(rows) == 5 and sum(counts) == len(recs))
    members = list(ring.values())
    outsider = [n for n in NAMES if n not in members][0]
    leaver = ring[2]
    print("ring:", ring, "| outside:", outsider, "| leaver:", leaver, flush=True)

    command(members[1], "query-dht %s 1" % members[1], "FAILURE")        # not Free
    command(members[1], "deregister %s" % members[1], "FAILURE")         # in DHT
    command(members[1], "teardown-dht %s" % members[1], "FAILURE")       # not leader
    command(outsider, "setup-dht %s 3 %d" % (outsider, year), "FAILURE")  # DHT exists

    # --- queries -----------------------------------------------------------
    for eid in ids:
        query(outsider, eid, eid in have)

    # --- leave-dht -----------------------------------------------------------
    send(leaver, "leave-dht %s" % leaver)
    check("leave-dht %s -> dht-rebuilt SUCCESS" % leaver,
          wait_count("manager", "'cmd': 'dht-rebuilt', 'status': 'SUCCESS'", 1))
    time.sleep(1)
    s = summaries()
    check("after leave: ring of 4 holds all records %s" % s[-1:],
          len(s) == 2 and s[-1][0] == s[-1][1] == str(len(recs)))
    _, states = last_table()
    check("leaver is Free again", states.get(leaver) == "Free")
    query(leaver, ids[0], ids[0] in have)

    # --- join-dht ------------------------------------------------------------
    send(outsider, "join-dht %s" % outsider)
    check("join-dht %s -> dht-rebuilt SUCCESS" % outsider,
          wait_count("manager", "'cmd': 'dht-rebuilt', 'status': 'SUCCESS'", 2))
    time.sleep(1)
    s = summaries()
    check("after join: ring of 5 holds all records %s" % s[-1:],
          len(s) == 3 and s[-1][0] == s[-1][1] == str(len(recs)))
    _, states = last_table()
    check("newcomer is InDHT", states.get(outsider) == "InDHT")
    query(leaver, ids[1], ids[1] in have)

    # --- teardown-dht ----------------------------------------------------------
    header, _ = last_table()
    leader = re.search(r"leader=(\w+)", header).group(1)
    command(leader, "teardown-dht %s" % leader, "SUCCESS")
    header, states = last_table()
    check("all peers Free and no DHT after teardown",
          "dht_exists=False" in header and set(states.values()) == {"Free"})
    command("Bob", "setup-dht Bob 3 %d" % year, "SUCCESS")      # a new DHT can be built
    check("second DHT built (3 nodes)", wait_count("manager", "'cmd': 'dht-complete', 'status': 'SUCCESS'", 2))
    time.sleep(1)
    header, _ = last_table()
    command("Bob", "teardown-dht Bob", "SUCCESS")

    # --- deregister --------------------------------------------------------------
    for n in NAMES:
        command(n, "deregister %s" % n, "SUCCESS")
    time.sleep(1.5)
    check("all peers exited", all(procs[n].poll() is not None for n in NAMES))
    check("manager table is empty", last_table()[1] == {})

    print("\n%d/%d checks passed" % (sum(results), len(results)))
    for p in procs.values():
        p.kill()
    for f in files.values():
        f.close()
    sys.exit(0 if all(results) else 1)


if __name__ == "__main__":
    main()
