# Testing Summary

## Environment

- Linux environment
- GCC with -std=c11 -Wall -Wextra -Wpedantic -O2 -pthread
- Agent: TCP port 9410
- Session tag: SID:0003
- UDP test port: 9411

## Test cases

| Test | Expected result | Observed result |
|---|---|---|
| Build with Makefile_000 | Both programs compile without warnings | PASS |
| SYSINFO before AUTH | Authentication-required error with SID:0003 | PASS |
| Wrong AUTH token | AUTH_FAILED error with SID:0003 | PASS |
| Correct AUTH token | AUTHENTICATED response with SID:0003 | PASS |
| SYSINFO | CPU, memory, uptime, and SID returned | PASS |
| LISTPROC | Process snapshot returned | PASS |
| EXEC DATE | Whitelisted result returned | PASS |
| EXEC RM | COMMAND_NOT_ALLOWED returned | PASS |
| PUT text file | File stored under personalised path | PASS |
| GET uploaded file | Downloaded file equals original byte-for-byte | PASS |
| GET missing file | FILE_NOT_FOUND returned | PASS |
| MONITOR START | MONITOR_STARTED and periodic UDP SYSINFO datagrams | PASS |
| MONITOR STOP | Datagrams stop and MONITOR_STOPPED returned | PASS |
| QUIT | BYE response and clean session close | PASS |
| Five simultaneous Controllers | All five complete AUTH/SYSINFO/EXEC/QUIT | PASS |

## Commands used

    make -f Makefile_000 clean all
    ./agent_000
    ./controller_000 127.0.0.1

The exact test commands and captured output are included in the implementation report. The student should repeat these tests and replace any result that differs on the final machine.
