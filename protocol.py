"""protocol.py - shared wire format for the DHT manager and peers.

Role in the protocol:
    Every message is ONE UDP datagram holding ONE UTF-8 JSON object with a
    "cmd" field. Both manager.py and peer.py import this module so encoding,
    decoding, timeouts and the request/response helper live in one place.

Usage:
    from protocol import encode, decode, request
"""

import json
import socket

SUCCESS = "SUCCESS"
FAILURE = "FAILURE"

MAX_DGRAM = 65535      # largest datagram we are willing to receive
RCVBUF = 1 << 20       # ~1 MB receive buffer so store bursts are not dropped
TIMEOUT = 2.0          # seconds to wait for a reply
RETRIES = 3            # retransmissions after the first attempt


def encode(msg):
    """Serialise a dict to UTF-8 JSON bytes (one datagram)."""
    return json.dumps(msg, separators=(",", ":")).encode("utf-8")


def decode(data):
    """Parse datagram bytes into a dict; return None if malformed."""
    try:
        msg = json.loads(data.decode("utf-8"))
        return msg if isinstance(msg, dict) and "cmd" in msg else None
    except (ValueError, UnicodeDecodeError):
        return None


def response(cmd, status, reason=None, **extra):
    """Build a manager reply: cmd echoes the request, status is SUCCESS/FAILURE."""
    msg = {"cmd": cmd, "status": status}
    if reason:
        msg["reason"] = reason
    msg.update(extra)
    return msg


def make_socket(port=0, ip="", rcvbuf=None):
    """Create a UDP socket bound to (ip, port); optionally enlarge SO_RCVBUF."""
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    if rcvbuf:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, rcvbuf)
    if port or ip:
        s.bind((ip, port))
    if hasattr(socket, "SIO_UDP_CONNRESET"):
        # Windows reports ICMP "port unreachable" as a recvfrom error; turn that off.
        s.ioctl(socket.SIO_UDP_CONNRESET, False)
    return s


def request(sock, addr, msg, match, timeout=TIMEOUT, retries=RETRIES, log=None):
    """Send msg to addr and block for a reply accepted by match(reply).

    Retry logic: UDP can lose datagrams, so we resend up to `retries` more
    times, waiting `timeout` seconds each attempt. Datagrams that do not
    satisfy match() are ignored (stale/unrelated). Returns the reply dict or
    None if every attempt timed out. Only for sockets used exclusively in a
    request/response fashion (the m-port).
    """
    import time
    for _ in range(retries + 1):
        if log:
            log("SEND", addr, msg)
        sock.sendto(encode(msg), addr)
        deadline = time.time() + timeout
        while True:
            left = deadline - time.time()
            if left <= 0:
                break
            sock.settimeout(left)
            try:
                data, src = sock.recvfrom(MAX_DGRAM)
            except socket.timeout:
                break
            reply = decode(data)
            if reply is not None and match(reply):
                if log:
                    log("RECV", src, reply)
                return reply
    return None
