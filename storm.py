"""storm.py - storm-event dataset loading and hashing helpers.

Role in the protocol:
    Used by the leader (Step 2) to read details-YYYY.csv, compute the local
    hash-table size s, and decide where each record belongs. Kept separate
    from peer.py so the full project (find-event, rebuild) can reuse it.

Usage:
    from storm import load_records, table_size, place
"""

import csv
import os

# The 14 fields, in the order required by the spec.
FIELDS = ["event_id", "state", "year", "month_name", "event_type", "cz_type",
          "cz_name", "injuries_direct", "injuries_indirect", "deaths_direct",
          "deaths_indirect", "damage_property", "damage_crops", "tor_f_scale"]

# Header names used by the full NOAA export, in the same order as FIELDS.
NOAA_NAMES = ["EVENT_ID", "STATE", "YEAR", "MONTH_NAME", "EVENT_TYPE",
              "CZ_TYPE", "CZ_NAME", "INJURIES_DIRECT", "INJURIES_INDIRECT",
              "DEATHS_DIRECT", "DEATHS_INDIRECT", "DAMAGE_PROPERTY",
              "DAMAGE_CROPS", "TOR_F_SCALE"]


def load_records(year, folder=None):
    """Load details-<year>.csv and return a list of 14-element lists.

    Header line is skipped. If the header contains the NOAA column names
    (51-column export) the 14 fields are picked by name; otherwise the file
    is assumed to hold only the 14 fields in order. Raises FileNotFoundError
    if the file is missing.
    """
    folder = folder or os.path.dirname(os.path.abspath(__file__))
    path = os.path.join(folder, "details-%s.csv" % year)
    with open(path, newline="", encoding="utf-8-sig") as f:
        rows = csv.reader(f)
        header = [h.strip().upper() for h in next(rows)]
        if all(n in header for n in NOAA_NAMES):
            idx = [header.index(n) for n in NOAA_NAMES]   # pick by name
        else:
            idx = list(range(14))                         # already 14 fields
        return [[r[i] for i in idx] for r in rows if len(r) > max(idx)]


def is_prime(k):
    """Trial-division primality test (fine for table sizes of this scale)."""
    if k < 2:
        return False
    i = 2
    while i * i <= k:
        if k % i == 0:
            return False
        i += 1
    return True


def table_size(l):
    """Return s = the first prime strictly greater than 2*l."""
    s = 2 * l + 1
    while not is_prime(s):
        s += 1
    return s


def place(event_id, s, n):
    """Return (pos, id): pos = event_id mod s; id = pos mod n (owning node)."""
    pos = int(event_id) % s
    return pos, pos % n
