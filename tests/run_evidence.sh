#!/bin/sh
set -eu

PROJECT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$PROJECT_DIR"
mkdir -p artifacts

make -f Makefile_000 clean all > artifacts/build_output.txt 2>&1

./agent_000 > artifacts/agent_startup.txt 2>&1 &
AGENT_PID=$!
cleanup() {
    kill "$AGENT_PID" 2>/dev/null || true
    wait "$AGENT_PID" 2>/dev/null || true
}
trap cleanup EXIT INT TERM

sleep 1

{
    printf 'auth\n'
    printf 'sysinfo\n'
    printf 'listproc\n'
    printf 'exec DATE\n'
    printf 'exec UPTIME\n'
    printf 'exec DISKFREE\n'
    printf 'exec HOSTNAME\n'
    printf 'exec WHOAMI\n'
    printf 'exec RM\n'
    printf 'put tests/sample_input.txt uploaded_evidence.txt\n'
    printf 'get uploaded_evidence.txt tests/downloaded_evidence.txt\n'
    printf 'monitor start 9411\n'
    sleep 3
    printf 'monitor stop\n'
    printf 'quit\n'
} | ./controller_000 127.0.0.1 > artifacts/controller_session.txt 2>&1

python3 - <<'PY' > artifacts/error_session.txt
import socket

sock = socket.create_connection(("127.0.0.1", 9410), timeout=3)
stream = sock.makefile("rwb", buffering=0)
for command in (b"SYSINFO\n", b"AUTH WRONG\n", b"AUTH OPS-3000\n",
                b"EXEC RM\n", b"GET missing_evidence.txt\n", b"QUIT\n"):
    stream.write(command)
    print(stream.readline().decode().rstrip())
sock.close()
PY

CLIENT_PIDS=""
for client_number in 1 2 3 4 5
do
    {
        printf 'auth\nsysinfo\nexec HOSTNAME\nquit\n'
    } | ./controller_000 127.0.0.1 > "artifacts/client_${client_number}.txt" 2>&1 &
    CLIENT_PIDS="$CLIENT_PIDS $!"
done
for client_pid in $CLIENT_PIDS
do
    wait "$client_pid"
done

if cmp -s tests/sample_input.txt tests/downloaded_evidence.txt
then
    echo "BYTE_COMPARE: PASS" > artifacts/byte_compare.txt
else
    echo "BYTE_COMPARE: FAIL" > artifacts/byte_compare.txt
fi

find agentfiles/IT24103000 -maxdepth 1 -type f -printf '%f %s bytes\n' > artifacts/storage_listing.txt
tail -n 40 remoteops_IT24103000.log > artifacts/log_excerpt.txt
