# Design Diary

## RemoteOps IT24103000

### Decision 1: Thread-per-connection concurrency

The Agent uses one detached POSIX thread for each accepted TCP Controller connection. This keeps the session state simple because each thread owns one socket, one authentication flag, and one monitoring state. It also satisfies the requirement to serve at least five simultaneous Controllers. A shared log mutex prevents concurrent threads from interleaving log entries.

### Decision 2: Buffered line and byte framing

TCP does not preserve application message boundaries. Therefore, both programs use a buffered reader that scans for a newline and retains any bytes following that newline. The same reader is used to transfer the exact PUT/GET byte count. This prevents a file byte that arrived in the same TCP packet as a command line from being lost.

### Decision 3: Direct system information and process enumeration

The Agent reads CPU load, memory information, and uptime from Linux /proc files. LISTPROC enumerates numeric directories under /proc and reads each process name from its comm file. This avoids arbitrary shell execution and keeps EXEC restricted to the five required names.

### Decision 4: UDP monitoring thread

MONITOR START creates a per-session UDP worker. It sends a SYSINFO-style datagram every two seconds to the Controller's IP address and requested UDP port. MONITOR STOP sets a mutex-protected stop flag and joins the worker before returning the response.

### Obstacles and fixes

During testing, process listing initially relied on a shell command. The sandboxed execution environment did not provide output through that subprocess path, so the implementation was changed to enumerate /proc directly. A prefix-length error in MONITOR START was also found by the integration test and corrected from 15 to 14 characters. These tests reinforced the importance of checking the exact wire protocol rather than testing only the user interface.

### Remaining personal actions

The student must add genuine GitHub commits, run the software in the department-compatible Linux environment, capture screenshots, and edit this diary so it accurately reflects the student's own development process.
