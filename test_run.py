"""test_run.py - end-to-end milestone test (manager + 3 peers as subprocesses).

Usage:
    python test_run.py

Starts manager.py and peers as subprocesses, feeds their stdin, saves each
process's output to logs/, and verifies the milestone requirements
(FAILURE cases, setup-dht SUCCESS, 72/75/76 counts, ring-only stores).
Peers use IP 127.0.0.1 or 127.0.0.2 to simulate two hosts.
"""

import os
import re
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
LOGS = os.path.join(HERE, "logs")
MGR_PORT = 1500
procs, files = {}, {}


def start(name, args):
    """Launch a python subprocess whose stdout/stderr go to logs/<name>.log."""
    f = open(os.path.join(LOGS, name + ".log"), "w", encoding="utf-8")
    p = subprocess.Popen([sys.executable, "-u"] + args, cwd=HERE, stdin=subprocess.PIPE,
                         stdout=f, stderr=subprocess.STDOUT, text=True)
    procs[name], files[name] = p, f


def read(name):
    """Return the current contents of a process log."""
    with open(os.path.join(LOGS, name + ".log"), encoding="utf-8") as f:
        return f.read()


def send(name, line, wait=0.8):
    """Type one command into a process and give it time to react."""
    procs[name].stdin.write(line + "\n")
    procs[name].stdin.flush()
    time.sleep(wait)


def last_status(name):
    """Status of the latest manager reply seen in a peer's log."""
    m = re.findall(r"RECV from [\d.]+:%d  .*?'status': '(\w+)'" % MGR_PORT, read(name))
    return m[-1] if m else None


def check(label, ok):
    """Print and record one verification result."""
    print(("PASS  " if ok else "FAIL  ") + label)
    results.append(ok)


results = []


def expect(label, name, status):
    """Assert the most recent reply to peer `name` has the given status."""
    check("%s -> %s" % (label, status), last_status(name) == status)


def main():
    """Run the scripted scenario and verify the logs."""
    os.makedirs(LOGS, exist_ok=True)
    start("manager", ["manager.py", str(MGR_PORT)])
    time.sleep(1)
    for nm in ("Alice", "Bob", "Charlie", "Tester1", "Tester2", "Tester3"):
        start(nm, ["peer.py", "127.0.0.1", str(MGR_PORT)])
    time.sleep(1)

    send("Alice", "register Alice 127.0.0.1 1501 1502")
    send("Bob", "register Bob 127.0.0.2 1503 1504")
    send("Charlie", "register Charlie 127.0.0.1 1505 1506")
    for nm in ("Alice", "Bob", "Charlie"):
        expect("register " + nm, nm, "SUCCESS")

    # --- FAILURE cases -------------------------------------------------
    send("Bob", "setup-dht Bob 2 1950")
    expect("setup-dht n=2", "Bob", "FAILURE")
    send("Bob", "setup-dht Bob 4 1950")
    expect("setup-dht n=4 with 3 peers", "Bob", "FAILURE")
    send("Tester1", "register Alice 127.0.0.1 1601 1602")
    expect("duplicate name", "Tester1", "FAILURE")
    send("Tester2", "register Dave1 127.0.0.1 1603 1604")
    expect("non-alphabetic name", "Tester2", "FAILURE")
    send("Tester3", "register Eve 127.0.0.1 1605 1502")
    expect("port already in use", "Tester3", "FAILURE")
    send("Bob", "setup-dht Zed 3 1950")
    expect("unregistered peer", "Bob", "FAILURE")

    # --- the real DHT --------------------------------------------------
    send("Alice", "setup-dht Alice 3 1950", wait=0.5)
    for _ in range(60):                       # wait for dht-complete SUCCESS
        if "'cmd': 'dht-complete', 'status': 'SUCCESS'" in read("manager"):
            break
        time.sleep(0.5)
    time.sleep(1)
    expect("setup-dht Alice 3 1950 / dht-complete", "Alice", "SUCCESS")
    send("Bob", "setup-dht Bob 3 1950")
    expect("second setup-dht (DHT exists)", "Bob", "FAILURE")
    check("reason mentions existing DHT", "a DHT already exists" in read("Bob"))

    # --- log checks ----------------------------------------------------
    leader = read("Alice")
    rows = re.findall(r"^\[Alice\] (\d)\s+(\w+)\s+([\d.]+):(\d+)\s+(\d+)\s*$", leader, re.M)
    cnt = {int(r[0]): (r[1], int(r[3]), int(r[4])) for r in rows}
    print("counts:", {k: v[2] for k, v in cnt.items()})
    check("counts are 72/75/76", [cnt.get(i, (0, 0, 0))[2] for i in range(3)] == [72, 75, 76])
    check("total 223", "total records: 223 (expected 223)" in leader)
    check("leader reports dht-complete SUCCESS", "'cmd': 'dht-complete', 'status': 'SUCCESS'" in leader)
    id1_port = cnt[1][1]
    sends = re.findall(r"SEND store event \d+ for id (\d) -> right neighbour [\d.]+:(\d+)", leader)
    check("every leader store goes to id-1 node's p-port",
          sends and all(int(p) == id1_port for _, p in sends))
    check("leader sent stores for id 2 only via id 1",
          any(i == "2" for i, _ in sends) and all(int(p) != cnt[2][1] for _, p in sends))
    check("no store came back to leader", "RECV store" not in leader)
    id1 = read(cnt[1][0])
    fwd = re.findall(r"RECV store event \d+ for id (\d) -> forwarded right", id1)
    check("id-1 node forwarded exactly the id-2 records",
          len(fwd) == cnt[2][2] and set(fwd) == {"2"})
    check("id-1 node stored only its own", len(re.findall(r"-> stored at pos", id1)) == cnt[1][2])
    print("\n%d/%d checks passed" % (sum(results), len(results)))

    for p in procs.values():
        p.kill()
    for f in files.values():
        f.close()
    sys.exit(0 if all(results) else 1)


if __name__ == "__main__":
    main()
