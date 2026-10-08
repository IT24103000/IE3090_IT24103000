#define _POSIX_C_SOURCE 200809L

#include <arpa/inet.h>
#include <ctype.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <netinet/in.h>
#include <pwd.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>
#include <stdarg.h>
#include <pthread.h>

#define REGISTRATION_NUMBER "IT24103000"
#define LISTEN_PORT 9410
#define SESSION_ID "0003"
#define AUTH_TOKEN "OPS-3000"
#define LOG_FILE "remoteops_IT24103000.log"
#define STORAGE_ROOT "./agentfiles/IT24103000"
#define MAX_FILE_SIZE (50ULL * 1024ULL * 1024ULL)
#define IO_BUFFER_SIZE 8192
#define COMMAND_BUFFER_SIZE 8192
#define MONITOR_INTERVAL_MS 2000

typedef struct {
    int fd;
    unsigned char buffer[IO_BUFFER_SIZE];
    size_t position;
    size_t length;
} buffered_reader_t;

typedef struct {
    int fd;
    struct sockaddr_in address;
    char peer_name[64];
    int authenticated;
    pthread_t monitor_thread;
    int monitor_active;
    int monitor_stop;
    int monitor_port;
    pthread_mutex_t monitor_mutex;
} client_session_t;

static pthread_mutex_t log_mutex = PTHREAD_MUTEX_INITIALIZER;

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

static int send_response_line(int fd, const char *format, ...) {
    char body[COMMAND_BUFFER_SIZE];
    char response[COMMAND_BUFFER_SIZE + 32];
    va_list args;

    va_start(args, format);
    vsnprintf(body, sizeof(body), format, args);
    va_end(args);

    snprintf(response, sizeof(response), "%s SID:%s\n", body, SESSION_ID);
    return send_all(fd, response, strlen(response));
}

static void log_event(const client_session_t *session, const char *format, ...) {
    FILE *file;
    time_t now = time(NULL);
    struct tm local_time;
    char timestamp[32];
    va_list args;

    localtime_r(&now, &local_time);
    strftime(timestamp, sizeof(timestamp), "%Y-%m-%d %H:%M:%S", &local_time);

    pthread_mutex_lock(&log_mutex);
    file = fopen(LOG_FILE, "a");
    if (file != NULL) {
        fprintf(file, "[%s] [%s] ", timestamp,
                session != NULL ? session->peer_name : "agent");
        va_start(args, format);
        vfprintf(file, format, args);
        va_end(args);
        fputc('\n', file);
        fclose(file);
    }
    pthread_mutex_unlock(&log_mutex);
}

static int ensure_storage_directory(void) {
    if (mkdir("./agentfiles", 0700) < 0 && errno != EEXIST) {
        return -1;
    }
    if (mkdir(STORAGE_ROOT, 0700) < 0 && errno != EEXIST) {
        return -1;
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

/* Reads one newline-terminated line while preserving any following bytes. */
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

/* Transfers exactly byte_count bytes from the buffered TCP stream. */
static int buffered_transfer_to_fd(buffered_reader_t *reader, int output_fd,
                                   unsigned long long byte_count) {
    unsigned char temporary[IO_BUFFER_SIZE];
    unsigned long long remaining = byte_count;

    while (remaining > 0) {
        size_t available;
        size_t chunk;
        ssize_t written;

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
            size_t offset = 0;
            while (offset < chunk) {
                written = write(output_fd, reader->buffer + reader->position + offset,
                                chunk - offset);
                if (written < 0 && errno == EINTR) {
                    continue;
                }
                if (written <= 0) {
                    return -1;
                }
                offset += (size_t)written;
            }
        } else {
            memcpy(temporary, reader->buffer + reader->position, chunk);
        }

        reader->position += chunk;
        remaining -= (unsigned long long)chunk;
    }
    return 0;
}

static int transfer_file_to_socket(int socket_fd, int input_fd, unsigned long long byte_count) {
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

static int valid_filename(const char *filename) {
    size_t length;
    size_t index;

    if (filename == NULL || filename[0] == '\0') {
        return 0;
    }
    length = strlen(filename);
    if (length > 255 || strcmp(filename, ".") == 0 || strcmp(filename, "..") == 0) {
        return 0;
    }
    if (strstr(filename, "..") != NULL || strchr(filename, '/') != NULL ||
        strchr(filename, '\\') != NULL) {
        return 0;
    }
    for (index = 0; index < length; index++) {
        if (isspace((unsigned char)filename[index]) ||
            iscntrl((unsigned char)filename[index])) {
            return 0;
        }
    }
    return 1;
}

static int make_storage_path(const char *filename, char *path, size_t capacity) {
    if (!valid_filename(filename)) {
        return -1;
    }
    if (snprintf(path, capacity, "%s/%s", STORAGE_ROOT, filename) >= (int)capacity) {
        return -1;
    }
    return 0;
}

static void get_system_info(char *output, size_t capacity) {
    double cpu_load = 0.0;
    long memory_total_kb = 0;
    long memory_available_kb = 0;
    double uptime_seconds = 0.0;
    FILE *file;
    char key[64];
    long value;
    char unit[16];

    file = fopen("/proc/loadavg", "r");
    if (file != NULL) {
        if (fscanf(file, "%lf", &cpu_load) != 1) {
            cpu_load = 0.0;
        }
        fclose(file);
    }

    file = fopen("/proc/meminfo", "r");
    if (file != NULL) {
        while (fscanf(file, "%63[^:]: %ld %15s\n", key, &value, unit) == 3) {
            if (strcmp(key, "MemTotal") == 0) {
                memory_total_kb = value;
            } else if (strcmp(key, "MemAvailable") == 0) {
                memory_available_kb = value;
            }
        }
        fclose(file);
    }

    file = fopen("/proc/uptime", "r");
    if (file != NULL) {
        if (fscanf(file, "%lf", &uptime_seconds) != 1) {
            uptime_seconds = 0.0;
        }
        fclose(file);
    }

    if (memory_available_kb > memory_total_kb) {
        memory_available_kb = memory_total_kb;
    }
    snprintf(output, capacity, "%.2f %ld %.0f",
             cpu_load,
             (memory_total_kb - memory_available_kb) / 1024L,
             uptime_seconds);
}

static void get_process_snapshot(char *output, size_t capacity) {
    DIR *proc_directory = opendir("/proc");
    struct dirent *entry;
    size_t used = 0;
    int first = 1;

    if (proc_directory == NULL) {
        snprintf(output, capacity, "unavailable");
        return;
    }

    while ((entry = readdir(proc_directory)) != NULL && used + 32 < capacity) {
        char process_path[PATH_MAX];
        char process_name[128];
        FILE *name_file;
        int written;

        if (!isdigit((unsigned char)entry->d_name[0])) {
            continue;
        }
        snprintf(process_path, sizeof(process_path), "/proc/%s/comm", entry->d_name);
        name_file = fopen(process_path, "r");
        if (name_file == NULL || fgets(process_name, sizeof(process_name), name_file) == NULL) {
            if (name_file != NULL) {
                fclose(name_file);
            }
            continue;
        }
        fclose(name_file);

        process_name[strcspn(process_name, "\r\n")] = '\0';
        written = snprintf(output + used, capacity - used,
                           "%s%s/%s", first ? "" : ",", entry->d_name, process_name);
        if (written < 0 || (size_t)written >= capacity - used) {
            break;
        }
        used += (size_t)written;
        first = 0;
    }
    closedir(proc_directory);
    if (first) {
        snprintf(output, capacity, "none");
    }
}

static int get_exec_result(const char *name, char *output, size_t capacity) {
    if (strcmp(name, "DATE") == 0) {
        time_t now = time(NULL);
        struct tm local_time;
        localtime_r(&now, &local_time);
        strftime(output, capacity, "%Y-%m-%d %H:%M:%S", &local_time);
        return 1;
    }

    if (strcmp(name, "UPTIME") == 0) {
        char info[128];
        char cpu_text[32];
        char memory_text[32];
        get_system_info(info, sizeof(info));
        if (sscanf(info, "%31s %31s %255s", cpu_text, memory_text, output) != 3) {
            snprintf(output, capacity, "unavailable");
        }
        return 1;
    }

    if (strcmp(name, "DISKFREE") == 0) {
        struct statvfs statistics;
        if (statvfs(".", &statistics) != 0) {
            snprintf(output, capacity, "unavailable");
        } else {
            unsigned long long free_bytes =
                (unsigned long long)statistics.f_bavail * statistics.f_frsize;
            snprintf(output, capacity, "%llu MB free", free_bytes / (1024ULL * 1024ULL));
        }
        return 1;
    }

    if (strcmp(name, "HOSTNAME") == 0) {
        if (gethostname(output, capacity) != 0) {
            snprintf(output, capacity, "unknown");
        } else {
            output[capacity - 1] = '\0';
        }
        return 1;
    }

    if (strcmp(name, "WHOAMI") == 0) {
        struct passwd *password = getpwuid(getuid());
        snprintf(output, capacity, "%s", password != NULL ? password->pw_name : "unknown");
        return 1;
    }

    return 0;
}

static int monitor_should_stop(client_session_t *session) {
    int stop;
    pthread_mutex_lock(&session->monitor_mutex);
    stop = session->monitor_stop;
    pthread_mutex_unlock(&session->monitor_mutex);
    return stop;
}

static void *monitor_worker(void *argument) {
    client_session_t *session = (client_session_t *)argument;
    int udp_socket;
    struct sockaddr_in destination = session->address;
    struct timespec delay;

    udp_socket = socket(AF_INET, SOCK_DGRAM, 0);
    if (udp_socket < 0) {
        log_event(session, "MONITOR socket creation failed: %s", strerror(errno));
        return NULL;
    }
    destination.sin_port = htons((uint16_t)session->monitor_port);
    delay.tv_sec = 0;
    delay.tv_nsec = 100000000L;

    while (!monitor_should_stop(session)) {
        char info[128];
        char datagram[256];
        int index;

        get_system_info(info, sizeof(info));
        snprintf(datagram, sizeof(datagram), "SYSINFO %s SID:%s", info, SESSION_ID);
        if (sendto(udp_socket, datagram, strlen(datagram), 0,
                   (struct sockaddr *)&destination, sizeof(destination)) < 0) {
            log_event(session, "MONITOR send failed: %s", strerror(errno));
            break;
        }
        for (index = 0; index < MONITOR_INTERVAL_MS / 100; index++) {
            if (monitor_should_stop(session)) {
                break;
            }
            nanosleep(&delay, NULL);
        }
    }

    close(udp_socket);
    return NULL;
}

static void stop_monitor(client_session_t *session) {
    pthread_t thread;
    int was_active;

    pthread_mutex_lock(&session->monitor_mutex);
    was_active = session->monitor_active;
    thread = session->monitor_thread;
    session->monitor_stop = 1;
    pthread_mutex_unlock(&session->monitor_mutex);

    if (was_active) {
        pthread_join(thread, NULL);
        pthread_mutex_lock(&session->monitor_mutex);
        session->monitor_active = 0;
        pthread_mutex_unlock(&session->monitor_mutex);
    }
}

static int start_monitor(client_session_t *session, int udp_port) {
    int result;

    stop_monitor(session);
    pthread_mutex_lock(&session->monitor_mutex);
    session->monitor_port = udp_port;
    session->monitor_stop = 0;
    result = pthread_create(&session->monitor_thread, NULL, monitor_worker, session);
    if (result == 0) {
        session->monitor_active = 1;
    }
    pthread_mutex_unlock(&session->monitor_mutex);
    return result == 0 ? 0 : -1;
}

static void handle_put(client_session_t *session, buffered_reader_t *reader,
                       const char *filename, unsigned long long file_size) {
    char path[PATH_MAX];
    int file_descriptor = -1;
    int valid_path = make_storage_path(filename, path, sizeof(path)) == 0;

    if (file_size > MAX_FILE_SIZE) {
        buffered_transfer_to_fd(reader, -1, file_size);
        send_response_line(session->fd, "ERR 004 FILE_TOO_LARGE");
        log_event(session, "PUT rejected filename=%s size=%llu reason=too_large",
                  filename, file_size);
        return;
    }

    if (!valid_path) {
        buffered_transfer_to_fd(reader, -1, file_size);
        send_response_line(session->fd, "ERR 006 INVALID_FILENAME");
        log_event(session, "PUT rejected filename=%s reason=invalid_filename", filename);
        return;
    }

    file_descriptor = open(path, O_WRONLY | O_CREAT | O_TRUNC, 0600);
    if (file_descriptor < 0) {
        buffered_transfer_to_fd(reader, -1, file_size);
        send_response_line(session->fd, "ERR 006 FILE_STORE_FAILED");
        log_event(session, "PUT failed filename=%s reason=%s", filename, strerror(errno));
        return;
    }

    if (buffered_transfer_to_fd(reader, file_descriptor, file_size) < 0) {
        close(file_descriptor);
        log_event(session, "PUT interrupted filename=%s", filename);
        return;
    }
    close(file_descriptor);
    send_response_line(session->fd, "OK FILE_RECEIVED %s", filename);
    log_event(session, "PUT filename=%s size=%llu", filename, file_size);
}

static void handle_get(client_session_t *session, const char *filename) {
    char path[PATH_MAX];
    struct stat file_status;
    int file_descriptor;
    unsigned long long file_size;

    if (make_storage_path(filename, path, sizeof(path)) != 0 ||
        stat(path, &file_status) != 0 || !S_ISREG(file_status.st_mode)) {
        send_response_line(session->fd, "ERR 005 FILE_NOT_FOUND");
        log_event(session, "GET filename=%s result=not_found", filename);
        return;
    }

    file_size = (unsigned long long)file_status.st_size;
    file_descriptor = open(path, O_RDONLY);
    if (file_descriptor < 0) {
        send_response_line(session->fd, "ERR 005 FILE_NOT_FOUND");
        log_event(session, "GET filename=%s result=open_failed", filename);
        return;
    }

    if (send_response_line(session->fd, "OK FILE_SEND %s %llu", filename, file_size) < 0 ||
        transfer_file_to_socket(session->fd, file_descriptor, file_size) < 0) {
        close(file_descriptor);
        log_event(session, "GET interrupted filename=%s", filename);
        return;
    }
    close(file_descriptor);
    log_event(session, "GET filename=%s size=%llu", filename, file_size);
}

static void *client_worker(void *argument) {
    client_session_t *session = (client_session_t *)argument;
    buffered_reader_t reader;
    char line[COMMAND_BUFFER_SIZE];
    int finished = 0;

    memset(&reader, 0, sizeof(reader));
    reader.fd = session->fd;
    log_event(session, "CONNECTION_OPEN");

    while (!finished) {
        int read_status = buffered_read_line(&reader, line, sizeof(line));
        if (read_status <= 0) {
            log_event(session, "DISCONNECT status=%s", read_status == 0 ? "clean" : "error");
            break;
        }
        log_event(session, "COMMAND %s", line);

        if (!session->authenticated) {
            if (strcmp(line, "AUTH " AUTH_TOKEN) == 0) {
                session->authenticated = 1;
                if (send_response_line(session->fd, "OK AUTHENTICATED") < 0) {
                    break;
                }
            } else if (strncmp(line, "AUTH ", 5) == 0) {
                if (send_response_line(session->fd, "ERR 001 AUTH_FAILED") < 0) {
                    break;
                }
            } else {
                if (send_response_line(session->fd, "ERR 001 AUTH_REQUIRED") < 0) {
                    break;
                }
            }
            continue;
        }

        if (strcmp(line, "SYSINFO") == 0) {
            char info[128];
            get_system_info(info, sizeof(info));
            if (send_response_line(session->fd, "OK SYSINFO %s", info) < 0) {
                break;
            }
        } else if (strcmp(line, "LISTPROC") == 0) {
            char processes[7000];
            get_process_snapshot(processes, sizeof(processes));
            if (send_response_line(session->fd, "OK PROCS %s", processes) < 0) {
                break;
            }
        } else if (strncmp(line, "EXEC ", 5) == 0) {
            char name[64];
            char result[256];
            if (sscanf(line + 5, "%63s", name) == 1 &&
                get_exec_result(name, result, sizeof(result))) {
                if (send_response_line(session->fd, "OK EXEC_RESULT %s", result) < 0) {
                    break;
                }
            } else if (send_response_line(session->fd, "ERR 002 COMMAND_NOT_ALLOWED") < 0) {
                break;
            }
        } else if (strncmp(line, "PUT ", 4) == 0) {
            char filename[256];
            unsigned long long file_size;
            if (sscanf(line + 4, "%255s %llu", filename, &file_size) != 2) {
                if (send_response_line(session->fd, "ERR 003 BAD_REQUEST") < 0) {
                    break;
                }
            } else {
                handle_put(session, &reader, filename, file_size);
            }
        } else if (strncmp(line, "GET ", 4) == 0) {
            char filename[256];
            if (sscanf(line + 4, "%255s", filename) != 1) {
                if (send_response_line(session->fd, "ERR 003 BAD_REQUEST") < 0) {
                    break;
                }
            } else {
                handle_get(session, filename);
            }
        } else if (strncmp(line, "MONITOR START ", 14) == 0) {
            int udp_port;
            char extra;
            if (sscanf(line + 14, "%d %c", &udp_port, &extra) != 1 ||
                udp_port < 1 || udp_port > 65535) {
                if (send_response_line(session->fd, "ERR 003 BAD_REQUEST") < 0) {
                    break;
                }
            } else if (start_monitor(session, udp_port) < 0) {
                if (send_response_line(session->fd, "ERR 007 MONITOR_FAILED") < 0) {
                    break;
                }
            } else {
                log_event(session, "MONITOR_START udp_port=%d interval_ms=%d",
                          udp_port, MONITOR_INTERVAL_MS);
                if (send_response_line(session->fd, "OK MONITOR_STARTED") < 0) {
                    break;
                }
            }
        } else if (strcmp(line, "MONITOR STOP") == 0) {
            stop_monitor(session);
            log_event(session, "MONITOR_STOP");
            if (send_response_line(session->fd, "OK MONITOR_STOPPED") < 0) {
                break;
            }
        } else if (strcmp(line, "QUIT") == 0) {
            stop_monitor(session);
            if (send_response_line(session->fd, "OK BYE") < 0) {
                break;
            }
            finished = 1;
        } else if (send_response_line(session->fd, "ERR 003 UNKNOWN_COMMAND") < 0) {
            break;
        }
    }

    stop_monitor(session);
    close(session->fd);
    log_event(session, "CONNECTION_CLOSED");
    pthread_mutex_destroy(&session->monitor_mutex);
    free(session);
    return NULL;
}

int main(void) {
    int server_socket;
    int reuse_address = 1;
    struct sockaddr_in server_address;

    signal(SIGPIPE, SIG_IGN);
    if (ensure_storage_directory() != 0) {
        perror("storage directory");
        return EXIT_FAILURE;
    }

    server_socket = socket(AF_INET, SOCK_STREAM, 0);
    if (server_socket < 0) {
        perror("socket");
        return EXIT_FAILURE;
    }
    setsockopt(server_socket, SOL_SOCKET, SO_REUSEADDR, &reuse_address,
               sizeof(reuse_address));

    memset(&server_address, 0, sizeof(server_address));
    server_address.sin_family = AF_INET;
    server_address.sin_addr.s_addr = htonl(INADDR_ANY);
    server_address.sin_port = htons(LISTEN_PORT);

    if (bind(server_socket, (struct sockaddr *)&server_address, sizeof(server_address)) < 0) {
        perror("bind");
        close(server_socket);
        return EXIT_FAILURE;
    }
    if (listen(server_socket, 16) < 0) {
        perror("listen");
        close(server_socket);
        return EXIT_FAILURE;
    }

    printf("RemoteOps Agent for %s listening on TCP port %d\n",
           REGISTRATION_NUMBER, LISTEN_PORT);
    printf("Storage: %s | SID: %s | Token: %s\n",
           STORAGE_ROOT, SESSION_ID, AUTH_TOKEN);
    fflush(stdout);

    for (;;) {
        struct sockaddr_in client_address;
        socklen_t client_length = sizeof(client_address);
        int client_socket = accept(server_socket,
                                   (struct sockaddr *)&client_address,
                                   &client_length);
        client_session_t *session;
        pthread_t thread;

        if (client_socket < 0) {
            if (errno == EINTR) {
                continue;
            }
            perror("accept");
            continue;
        }

        session = calloc(1, sizeof(*session));
        if (session == NULL) {
            close(client_socket);
            continue;
        }
        session->fd = client_socket;
        session->address = client_address;
        inet_ntop(AF_INET, &client_address.sin_addr,
                  session->peer_name, sizeof(session->peer_name));
        snprintf(session->peer_name + strlen(session->peer_name),
                 sizeof(session->peer_name) - strlen(session->peer_name),
                 ":%u", ntohs(client_address.sin_port));
        pthread_mutex_init(&session->monitor_mutex, NULL);

        if (pthread_create(&thread, NULL, client_worker, session) != 0) {
            pthread_mutex_destroy(&session->monitor_mutex);
            close(client_socket);
            free(session);
            continue;
        }
        pthread_detach(thread);
    }

    close(server_socket);
    return EXIT_SUCCESS;
}
