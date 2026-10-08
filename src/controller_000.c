#define _POSIX_C_SOURCE 200809L

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <netdb.h>
#include <netinet/in.h>
#include <pthread.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>

#define REGISTRATION_NUMBER "IT24103000"
#define AGENT_PORT 9410
#define SESSION_ID "0003"
#define AUTH_TOKEN "OPS-3000"
#define IO_BUFFER_SIZE 8192
#define COMMAND_BUFFER_SIZE 8192

typedef struct {
    int fd;
    unsigned char buffer[IO_BUFFER_SIZE];
    size_t position;
    size_t length;
} buffered_reader_t;

typedef struct {
    int socket_fd;
    int port;
    int stop;
    pthread_mutex_t mutex;
} udp_monitor_t;

static int send_all(int fd, const void *data, size_t length) {
    const unsigned char *bytes = (const unsigned char *)data;
    size_t sent = 0;

    while (sent < length) {
        ssize_t count = send(fd, bytes + sent, length - sent, MSG_NOSIGNAL);
        if (count < 0) {
            if (errno == EINTR) {
                continue;
            }
            return -1;
        }
        if (count == 0) {
            return -1;
        }
        sent += (size_t)count;
    }
    return 0;
}

static int buffered_fill(buffered_reader_t *reader) {
    ssize_t count;

    reader->position = 0;
    reader->length = 0;
    do {
        count = recv(reader->fd, reader->buffer, sizeof(reader->buffer), 0);
    } while (count < 0 && errno == EINTR);

    if (count <= 0) {
        return (int)count;
    }
    reader->length = (size_t)count;
    return 1;
}

static int buffered_read_line(buffered_reader_t *reader, char *output, size_t capacity) {
    size_t used = 0;

    for (;;) {
        if (reader->position == reader->length) {
            int filled = buffered_fill(reader);
            if (filled <= 0) {
                return used == 0 ? filled : -1;
            }
        }

        while (reader->position < reader->length) {
            unsigned char character = reader->buffer[reader->position++];
            if (character == '\n') {
                if (used > 0 && output[used - 1] == '\r') {
                    used--;
                }
                output[used] = '\0';
                return 1;
            }
            if (used + 1 >= capacity) {
                return -1;
            }
            output[used++] = (char)character;
        }
    }
}

static int buffered_transfer_to_fd(buffered_reader_t *reader, int output_fd,
                                   unsigned long long byte_count) {
    unsigned long long remaining = byte_count;

    while (remaining > 0) {
        size_t available;
        size_t chunk;
        size_t offset = 0;

        if (reader->position == reader->length) {
            if (buffered_fill(reader) <= 0) {
                return -1;
            }
        }

        available = reader->length - reader->position;
        chunk = available;
        if ((unsigned long long)chunk > remaining) {
            chunk = (size_t)remaining;
        }

        if (output_fd >= 0) {
            while (offset < chunk) {
                ssize_t written = write(output_fd,
                                        reader->buffer + reader->position + offset,
                                        chunk - offset);
                if (written < 0 && errno == EINTR) {
                    continue;
                }
                if (written <= 0) {
                    return -1;
                }
                offset += (size_t)written;
            }
        }
        reader->position += chunk;
        remaining -= (unsigned long long)chunk;
    }
    return 0;
}

static int transfer_file_to_socket(int socket_fd, int input_fd,
                                   unsigned long long byte_count) {
    unsigned char buffer[IO_BUFFER_SIZE];
    unsigned long long remaining = byte_count;

    while (remaining > 0) {
        size_t requested = sizeof(buffer);
        ssize_t count;
        if ((unsigned long long)requested > remaining) {
            requested = (size_t)remaining;
        }

        do {
            count = read(input_fd, buffer, requested);
        } while (count < 0 && errno == EINTR);

        if (count <= 0 || send_all(socket_fd, buffer, (size_t)count) < 0) {
            return -1;
        }
        remaining -= (unsigned long long)count;
    }
    return 0;
}

static int connect_to_agent(const char *host) {
    char port_text[16];
    struct addrinfo hints;
    struct addrinfo *results = NULL;
    struct addrinfo *candidate;
    int socket_fd = -1;

    snprintf(port_text, sizeof(port_text), "%d", AGENT_PORT);
    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;

    if (getaddrinfo(host, port_text, &hints, &results) != 0) {
        return -1;
    }

    for (candidate = results; candidate != NULL; candidate = candidate->ai_next) {
        socket_fd = socket(candidate->ai_family, candidate->ai_socktype,
                           candidate->ai_protocol);
        if (socket_fd < 0) {
            continue;
        }
        if (connect(socket_fd, candidate->ai_addr, candidate->ai_addrlen) == 0) {
            break;
        }
        close(socket_fd);
        socket_fd = -1;
    }
    freeaddrinfo(results);
    return socket_fd;
}

static int is_response_ok(const char *line) {
    return strncmp(line, "OK ", 3) == 0;
}

static int receive_and_print(buffered_reader_t *reader) {
    char response[COMMAND_BUFFER_SIZE];
    int status = buffered_read_line(reader, response, sizeof(response));
    if (status <= 0) {
        fprintf(stderr, "Agent disconnected while waiting for a response.\n");
        return -1;
    }
    printf("AGENT: %s\n", response);
    return 0;
}

static int start_udp_monitor(udp_monitor_t *monitor, int port, pthread_t *thread);
static void stop_udp_monitor(udp_monitor_t *monitor, pthread_t thread, int active);

static void *udp_monitor_worker(void *argument) {
    udp_monitor_t *monitor = (udp_monitor_t *)argument;
    struct sockaddr_in local_address;
    char buffer[512];

    monitor->socket_fd = socket(AF_INET, SOCK_DGRAM, 0);
    if (monitor->socket_fd < 0) {
        return NULL;
    }

    memset(&local_address, 0, sizeof(local_address));
    local_address.sin_family = AF_INET;
    local_address.sin_addr.s_addr = htonl(INADDR_ANY);
    local_address.sin_port = htons((uint16_t)monitor->port);

    if (bind(monitor->socket_fd, (struct sockaddr *)&local_address,
             sizeof(local_address)) < 0) {
        close(monitor->socket_fd);
        monitor->socket_fd = -1;
        return NULL;
    }

    while (1) {
        struct timeval timeout;
        ssize_t count;
        int stopped;

        pthread_mutex_lock(&monitor->mutex);
        stopped = monitor->stop;
        pthread_mutex_unlock(&monitor->mutex);
        if (stopped) {
            break;
        }

        timeout.tv_sec = 1;
        timeout.tv_usec = 0;
        setsockopt(monitor->socket_fd, SOL_SOCKET, SO_RCVTIMEO,
                   &timeout, sizeof(timeout));
        count = recvfrom(monitor->socket_fd, buffer, sizeof(buffer) - 1, 0,
                         NULL, NULL);
        if (count > 0) {
            buffer[count] = '\0';
            printf("UDP: %s\n", buffer);
            fflush(stdout);
        }
    }

    close(monitor->socket_fd);
    monitor->socket_fd = -1;
    return NULL;
}

static int start_udp_monitor(udp_monitor_t *monitor, int port, pthread_t *thread) {
    pthread_mutex_init(&monitor->mutex, NULL);
    monitor->socket_fd = -1;
    monitor->port = port;
    monitor->stop = 0;
    if (pthread_create(thread, NULL, udp_monitor_worker, monitor) != 0) {
        pthread_mutex_destroy(&monitor->mutex);
        return -1;
    }
    return 0;
}

static void stop_udp_monitor(udp_monitor_t *monitor, pthread_t thread, int active) {
    if (!active) {
        return;
    }
    pthread_mutex_lock(&monitor->mutex);
    monitor->stop = 1;
    pthread_mutex_unlock(&monitor->mutex);
    pthread_join(thread, NULL);
    pthread_mutex_destroy(&monitor->mutex);
}

static int send_simple_command(buffered_reader_t *reader, const char *command) {
    char line[COMMAND_BUFFER_SIZE];
    snprintf(line, sizeof(line), "%s\n", command);
    if (send_all(reader->fd, line, strlen(line)) < 0) {
        return -1;
    }
    return receive_and_print(reader);
}

static int perform_put(buffered_reader_t *reader, const char *local_path,
                       const char *remote_name) {
    struct stat file_status;
    int input_fd;
    char command[COMMAND_BUFFER_SIZE];

    if (stat(local_path, &file_status) != 0 || !S_ISREG(file_status.st_mode)) {
        fprintf(stderr, "Local file does not exist or is not a regular file.\n");
        return 0;
    }
    if (file_status.st_size < 0) {
        fprintf(stderr, "Invalid local file size.\n");
        return 0;
    }

    input_fd = open(local_path, O_RDONLY);
    if (input_fd < 0) {
        perror("open local file");
        return 0;
    }

    snprintf(command, sizeof(command), "PUT %s %llu\n", remote_name,
             (unsigned long long)file_status.st_size);
    if (send_all(reader->fd, command, strlen(command)) < 0 ||
        transfer_file_to_socket(reader->fd, input_fd,
                                (unsigned long long)file_status.st_size) < 0) {
        close(input_fd);
        return -1;
    }
    close(input_fd);
    return receive_and_print(reader);
}

static int perform_get(buffered_reader_t *reader, const char *remote_name,
                       const char *local_path) {
    char command[COMMAND_BUFFER_SIZE];
    char response[COMMAND_BUFFER_SIZE];
    char returned_name[256];
    char sid[32];
    unsigned long long file_size;
    int output_fd;
    int status;

    snprintf(command, sizeof(command), "GET %s\n", remote_name);
    if (send_all(reader->fd, command, strlen(command)) < 0) {
        return -1;
    }
    status = buffered_read_line(reader, response, sizeof(response));
    if (status <= 0) {
        fprintf(stderr, "Agent disconnected during GET.\n");
        return -1;
    }
    if (!is_response_ok(response)) {
        printf("AGENT: %s\n", response);
        return 0;
    }

    if (sscanf(response, "OK FILE_SEND %255s %llu SID:%31s",
               returned_name, &file_size, sid) != 3) {
        fprintf(stderr, "Malformed GET response.\n");
        return -1;
    }

    output_fd = open(local_path, O_WRONLY | O_CREAT | O_TRUNC, 0600);
    if (output_fd < 0) {
        perror("open download path");
        return -1;
    }
    if (buffered_transfer_to_fd(reader, output_fd, file_size) < 0) {
        close(output_fd);
        return -1;
    }
    close(output_fd);
    printf("AGENT: OK FILE_RECEIVED_LOCAL %s %llu bytes SID:%s\n",
           local_path, file_size, sid);
    return 0;
}

static void print_help(void) {
    puts("Commands:");
    puts("  auth                         Authenticate with the personalised token");
    puts("  sysinfo                      Request CPU, memory, and uptime");
    puts("  listproc                     Request a process snapshot");
    puts("  exec DATE|UPTIME|DISKFREE|HOSTNAME|WHOAMI");
    puts("  put <local_path> [remote_name]");
    puts("  get <remote_name> [local_path]");
    puts("  monitor start <udp_port>");
    puts("  monitor stop");
    puts("  quit");
}

int main(int argc, char **argv) {
    const char *host = argc > 1 ? argv[1] : "127.0.0.1";
    int socket_fd;
    buffered_reader_t reader;
    char input[COMMAND_BUFFER_SIZE];
    int monitor_active = 0;
    udp_monitor_t monitor;
    pthread_t monitor_thread;

    signal(SIGPIPE, SIG_IGN);
    socket_fd = connect_to_agent(host);
    if (socket_fd < 0) {
        fprintf(stderr, "Unable to connect to %s:%d\n", host, AGENT_PORT);
        return EXIT_FAILURE;
    }

    memset(&reader, 0, sizeof(reader));
    reader.fd = socket_fd;
    printf("Connected to RemoteOps Agent %s:%d (SID:%s expected).\n",
           host, AGENT_PORT, SESSION_ID);
    print_help();

    while (1) {
        char command[64];
        char argument1[PATH_MAX];
        char argument2[PATH_MAX];
        int udp_port;

        printf("remoteops> ");
        fflush(stdout);
        if (fgets(input, sizeof(input), stdin) == NULL) {
            break;
        }
        input[strcspn(input, "\r\n")] = '\0';
        if (input[0] == '\0') {
            continue;
        }

        if (strcmp(input, "help") == 0) {
            print_help();
        } else if (strcmp(input, "auth") == 0) {
            send_simple_command(&reader, "AUTH " AUTH_TOKEN);
        } else if (strcmp(input, "sysinfo") == 0) {
            send_simple_command(&reader, "SYSINFO");
        } else if (strcmp(input, "listproc") == 0) {
            send_simple_command(&reader, "LISTPROC");
        } else if (sscanf(input, "exec %63s", command) == 1 &&
                   strncmp(input, "exec ", 5) == 0) {
            char protocol_command[128];
            snprintf(protocol_command, sizeof(protocol_command), "EXEC %s", command);
            send_simple_command(&reader, protocol_command);
        } else if (sscanf(input, "put %4095s %4095s", argument1, argument2) == 2) {
            perform_put(&reader, argument1, argument2);
        } else if (sscanf(input, "put %4095s", argument1) == 1) {
            const char *last_slash = strrchr(argument1, '/');
            perform_put(&reader, argument1,
                        last_slash != NULL ? last_slash + 1 : argument1);
        } else if (sscanf(input, "get %4095s %4095s", argument1, argument2) == 2) {
            perform_get(&reader, argument1, argument2);
        } else if (sscanf(input, "get %4095s", argument1) == 1) {
            perform_get(&reader, argument1, argument1);
        } else if (sscanf(input, "monitor start %d", &udp_port) == 1) {
            if (udp_port < 1 || udp_port > 65535) {
                puts("UDP port must be between 1 and 65535.");
                continue;
            }
            if (monitor_active) {
                stop_udp_monitor(&monitor, monitor_thread, monitor_active);
                monitor_active = 0;
            }
            if (start_udp_monitor(&monitor, udp_port, &monitor_thread) != 0) {
                puts("Unable to start the UDP listener.");
                continue;
            }
            {
                char protocol_command[64];
                snprintf(protocol_command, sizeof(protocol_command),
                         "MONITOR START %d", udp_port);
                if (send_simple_command(&reader, protocol_command) != 0) {
                    stop_udp_monitor(&monitor, monitor_thread, 1);
                    monitor_active = 0;
                    break;
                }
            }
            monitor_active = 1;
        } else if (strcmp(input, "monitor stop") == 0) {
            send_simple_command(&reader, "MONITOR STOP");
            if (monitor_active) {
                stop_udp_monitor(&monitor, monitor_thread, monitor_active);
                monitor_active = 0;
            }
        } else if (strcmp(input, "quit") == 0) {
            send_simple_command(&reader, "QUIT");
            break;
        } else {
            puts("Unknown local command. Type help.");
        }
    }

    if (monitor_active) {
        stop_udp_monitor(&monitor, monitor_thread, monitor_active);
    }
    close(socket_fd);
    return EXIT_SUCCESS;
}
