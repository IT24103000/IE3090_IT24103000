# Protocol Test Commands

## Build

    make -f Makefile_000 clean all

## Manual session

Terminal 1:

    ./agent_000

Terminal 2:

    ./controller_000 127.0.0.1

Then enter:

    auth
    sysinfo
    listproc
    exec DATE
    exec UPTIME
    exec DISKFREE
    exec HOSTNAME
    exec WHOAMI
    put tests/sample_input.txt uploaded.txt
    get uploaded.txt tests/downloaded.txt
    monitor start 9411
    monitor stop
    quit

## Personalisation evidence

    ss -tlnp | rg ':9410'
    tail -n 20 remoteops_IT24103000.log
    find agentfiles/IT24103000 -maxdepth 1 -type f -printf '%f %s bytes\n'

If ss is unavailable, use netstat -tlnp or show the Agent startup line.
