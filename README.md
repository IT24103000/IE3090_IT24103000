# RemoteOps IT24103000

RemoteOps is a C client/server system-monitoring and file-management tool for IE3090 Network Programming. The Agent is the TCP server running on the managed machine. The Controller is the TCP client used by an administrator. The Agent also sends periodic monitoring datagrams through UDP.

## Personalised values

| Item | Value |
|---|---|
| Registration number | IT24103000 |
| Numeric part | 24103000 |
| First four numeric digits | 2410 |
| Agent TCP port | 9410 |
| Last three digits | 000 |
| Source files | agent_000.c and controller_000.c |
| Makefile | Makefile_000 |
| Last four digits | 3000 |
| Reversed last four digits | 0003 |
| Session tag | SID:0003 |
| Authentication token | OPS-3000 |
| Log file | remoteops_IT24103000.log |
| Storage directory | ./agentfiles/IT24103000/ |
| Submission archive | IE3090_IT24103000.zip |

## Build

Use a Linux machine with GCC and POSIX threads:

    make -f Makefile_000

The build uses:

    gcc -std=c11 -Wall -Wextra -Wpedantic -O2 -pthread

## Run

Open two terminals in the project directory.

Terminal 1:

    ./agent_000

Terminal 2:

    ./controller_000 127.0.0.1

The Agent creates ./agentfiles/IT24103000/ automatically. Uploaded files are stored inside that directory. The Agent writes connection, command, monitoring, and file-transfer events to remoteops_IT24103000.log.

## Controller commands

    auth
    sysinfo
    listproc
    exec DATE
    exec UPTIME
    exec DISKFREE
    exec HOSTNAME
    exec WHOAMI
    put <local_file> [remote_filename]
    get <remote_filename> [local_file]
    monitor start <udp_port>
    monitor stop
    quit

The Controller converts these local commands into the exact protocol commands required by the brief.

## Architecture

The Agent creates one detached POSIX thread for every accepted TCP Controller connection. Each session owns its authentication state and UDP monitoring thread. A mutex protects the monitoring state, while a separate mutex serialises log-file writes. The Controller uses a buffered TCP reader so that partial lines, multiple lines in one TCP segment, and file bytes following a PUT/GET header are handled correctly.

The Agent reads system information from Linux /proc, enumerates processes from /proc, executes only the five permitted operations through direct C/POSIX calls, and never passes a user-supplied command to a shell. Filenames are validated to prevent path traversal.

## Protocol notes

All TCP response lines end with " SID:0003\n". PUT and GET transfer exactly the advertised number of raw bytes after the response/request line. UDP monitoring messages use the form:

    SYSINFO <cpu_load> <mem_used_mb> <uptime_sec> SID:0003

The monitoring interval is 2 seconds.

## Evidence and submission preparation

The docs/ directory contains the design diary, prompt log, reflection, testing summary, and submission checklist. The implementation report is generated separately as a Word document and PDF.

Before submission:

1. Run the program on a Linux environment and capture genuine screenshots of your own execution.
2. Check the calculated port, SID, token, filenames, log, and storage directory.
3. Add your own GitHub repository link and make meaningful commits with descriptive messages.
4. Read and understand every function before the viva and lab assessment.
5. Replace any draft evidence or wording that does not accurately describe your own testing.

Do not submit code that you cannot explain.
