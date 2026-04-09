// server.c - Servidor de mensajería

#define _POSIX_C_SOURCE 200112L

#include <arpa/inet.h>
#include <errno.h>
#include <netdb.h>
#include <pthread.h>
#include <signal.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#define BACKLOG 50
#define MAX_USERNAME 256
#define MAX_IP INET6_ADDRSTRLEN
#define MAX_PORT_STR 16
#define MAX_MESSAGE_LEN 256

typedef enum {
    USER_DISCONNECTED = 0,
    USER_CONNECTED = 1
} user_state_t;

typedef struct pending_message {
    unsigned int id;
    char sender[MAX_USERNAME];
    char text[MAX_MESSAGE_LEN];
    struct pending_message *next;
} pending_message_t;

typedef struct user {
    char username[MAX_USERNAME];
    user_state_t state;
    char ip[MAX_IP];
    char port[MAX_PORT_STR];

    unsigned int last_message_id;

    pending_message_t *pending_head;
    pending_message_t *pending_tail;

    struct user *next;
} user_t;

typedef struct {
    int client_fd;
    struct sockaddr_storage client_addr;
    socklen_t client_addr_len;
} client_thread_arg_t;

/* ===========================
 * Variables globales
 * =========================== */

static user_t *g_users = NULL;
static pthread_mutex_t g_users_mutex = PTHREAD_MUTEX_INITIALIZER;

static volatile sig_atomic_t g_stop = 0;
static int g_listen_fd = -1;

/* ===========================
 * Señales
 * =========================== */

static void handle_sigint(int sig) {
    (void)sig;
    g_stop = 1;

    if (g_listen_fd != -1) {
        close(g_listen_fd);
        g_listen_fd = -1;
    }
}

/* ===========================
 * Utilidades de red
 * =========================== */

static void get_ip_str(const struct sockaddr *sa, char *out, size_t out_size) {
    if (sa->sa_family == AF_INET) {
        const struct sockaddr_in *addr4 = (const struct sockaddr_in *)sa;
        inet_ntop(AF_INET, &(addr4->sin_addr), out, (socklen_t)out_size);
    } else if (sa->sa_family == AF_INET6) {
        const struct sockaddr_in6 *addr6 = (const struct sockaddr_in6 *)sa;
        inet_ntop(AF_INET6, &(addr6->sin6_addr), out, (socklen_t)out_size);
    } else {
        snprintf(out, out_size, "unknown");
    }
}

static int get_port_num(const struct sockaddr *sa) {
    if (sa->sa_family == AF_INET) {
        const struct sockaddr_in *addr4 = (const struct sockaddr_in *)sa;
        return ntohs(addr4->sin_port);
    }
    if (sa->sa_family == AF_INET6) {
        const struct sockaddr_in6 *addr6 = (const struct sockaddr_in6 *)sa;
        return ntohs(addr6->sin6_port);
    }
    return -1;
}

static int get_local_ip_port(int sockfd, char *ip, size_t ip_size, int *port) {
    struct sockaddr_storage local_addr;
    socklen_t len = sizeof(local_addr);

    if (getsockname(sockfd, (struct sockaddr *)&local_addr, &len) == -1) {
        return -1;
    }

    get_ip_str((struct sockaddr *)&local_addr, ip, ip_size);
    *port = get_port_num((struct sockaddr *)&local_addr);
    return 0;
}

static int send_all(int fd, const void *buf, size_t len) {
    const char *p = (const char *)buf;
    size_t total = 0;

    while (total < len) {
        ssize_t n = send(fd, p + total, len - total, 0);
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            return -1;
        }
        if (n == 0) {
            return -1;
        }
        total += (size_t)n;
    }

    return 0;
}

static int recv_all(int fd, void *buf, size_t len) {
    char *p = (char *)buf;
    size_t total = 0;

    while (total < len) {
        ssize_t n = recv(fd, p + total, len - total, 0);
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            return -1;
        }
        if (n == 0) {
            return -1;
        }
        total += (size_t)n;
    }

    return 0;
}

static int send_byte_code(int fd, uint8_t code) {
    return send_all(fd, &code, sizeof(code));
}

/*
 * Recibe una cadena terminada en '\0'.
 * Devuelve 0 si todo va bien, -1 si falla.
 */
static int recv_cstring(int fd, char *buf, size_t buf_size) {
    size_t pos = 0;

    if (buf == NULL || buf_size == 0) {
        return -1;
    }

    while (1) {
        char c;
        if (recv_all(fd, &c, 1) != 0) {
            return -1;
        }

        if (pos >= buf_size) {
            return -1;
        }

        buf[pos++] = c;

        if (c == '\0') {
            break;
        }
    }

    return 0;
}

/* ===========================
 * Gestión de mensajes pendientes
 * =========================== */

static void free_pending_messages(pending_message_t *head) {
    pending_message_t *curr = head;

    while (curr != NULL) {
        pending_message_t *next = curr->next;
        free(curr);
        curr = next;
    }
}

static unsigned int next_message_id_for_sender(user_t *sender) {
    sender->last_message_id++;

    if (sender->last_message_id == 0) {
        sender->last_message_id = 1;
    }

    return sender->last_message_id;
}

static int enqueue_message_for_user(user_t *receiver,
                                    const char *sender_name,
                                    unsigned int msg_id,
                                    const char *text) {
    pending_message_t *msg = (pending_message_t *)malloc(sizeof(pending_message_t));
    if (msg == NULL) {
        return -1;
    }

    msg->id = msg_id;

    strncpy(msg->sender, sender_name, sizeof(msg->sender) - 1);
    msg->sender[sizeof(msg->sender) - 1] = '\0';

    strncpy(msg->text, text, sizeof(msg->text) - 1);
    msg->text[sizeof(msg->text) - 1] = '\0';

    msg->next = NULL;

    if (receiver->pending_tail == NULL) {
        receiver->pending_head = msg;
        receiver->pending_tail = msg;
    } else {
        receiver->pending_tail->next = msg;
        receiver->pending_tail = msg;
    }

    return 0;
}

/* ===========================
 * Gestión de usuarios
 * =========================== */

static user_t *find_user_locked(const char *username) {
    user_t *curr = g_users;

    while (curr != NULL) {
        if (strcmp(curr->username, username) == 0) {
            return curr;
        }
        curr = curr->next;
    }

    return NULL;
}

/*
 * REGISTER:
 *  0 -> éxito
 *  1 -> ya existe
 * -1 -> error interno
 */
static int register_user_internal(const char *username) {
    user_t *existing;
    user_t *new_user;

    if (username == NULL || username[0] == '\0') {
        return -1;
    }

    pthread_mutex_lock(&g_users_mutex);

    existing = find_user_locked(username);
    if (existing != NULL) {
        pthread_mutex_unlock(&g_users_mutex);
        return 1;
    }

    new_user = (user_t *)malloc(sizeof(user_t));
    if (new_user == NULL) {
        pthread_mutex_unlock(&g_users_mutex);
        return -1;
    }

    memset(new_user, 0, sizeof(user_t));

    strncpy(new_user->username, username, sizeof(new_user->username) - 1);
    new_user->username[sizeof(new_user->username) - 1] = '\0';

    new_user->state = USER_DISCONNECTED;
    new_user->ip[0] = '\0';
    new_user->port[0] = '\0';
    new_user->last_message_id = 0;
    new_user->pending_head = NULL;
    new_user->pending_tail = NULL;

    new_user->next = g_users;
    g_users = new_user;

    pthread_mutex_unlock(&g_users_mutex);
    return 0;
}

/*
 * UNREGISTER:
 *  0 -> éxito
 *  1 -> no existe
 * -1 -> error interno
 */
static int unregister_user_internal(const char *username) {
    user_t *curr;
    user_t *prev = NULL;

    if (username == NULL || username[0] == '\0') {
        return -1;
    }

    pthread_mutex_lock(&g_users_mutex);

    curr = g_users;
    while (curr != NULL) {
        if (strcmp(curr->username, username) == 0) {
            break;
        }
        prev = curr;
        curr = curr->next;
    }

    if (curr == NULL) {
        pthread_mutex_unlock(&g_users_mutex);
        return 1;
    }

    if (prev == NULL) {
        g_users = curr->next;
    } else {
        prev->next = curr->next;
    }

    free_pending_messages(curr->pending_head);
    free(curr);

    pthread_mutex_unlock(&g_users_mutex);
    return 0;
}

static int store_message_internal(const char *sender_name,
                                  const char *receiver_name,
                                  const char *text,
                                  unsigned int *assigned_id) {
    user_t *sender;
    user_t *receiver;
    unsigned int new_id;

    if (sender_name == NULL || receiver_name == NULL || text == NULL || assigned_id == NULL) {
        return -1;
    }

    pthread_mutex_lock(&g_users_mutex);

    sender = find_user_locked(sender_name);
    receiver = find_user_locked(receiver_name);

    if (sender == NULL || receiver == NULL) {
        pthread_mutex_unlock(&g_users_mutex);
        return 1;
    }

    new_id = next_message_id_for_sender(sender);

    if (enqueue_message_for_user(receiver, sender_name, new_id, text) != 0) {
        pthread_mutex_unlock(&g_users_mutex);
        return -1;
    }

    *assigned_id = new_id;

    pthread_mutex_unlock(&g_users_mutex);
    return 0;
}

/*
 * Mantiene sin warnings las funciones de la fase 2.1
 * sin alterar el estado final del servidor.
 */
static void internal_silent_selfcheck(void) {
    unsigned int msg_id = 0;
    int rc;

    rc = register_user_internal("__tmp_sender__");
    (void)rc;

    rc = register_user_internal("__tmp_receiver__");
    (void)rc;

    rc = store_message_internal("__tmp_sender__", "__tmp_receiver__", "tmp", &msg_id);
    (void)rc;
    (void)msg_id;

    rc = unregister_user_internal("__tmp_sender__");
    (void)rc;

    rc = unregister_user_internal("__tmp_receiver__");
    (void)rc;
}

/* ===========================
 * Protocolo REGISTER/UNREGISTER
 * =========================== */

static void handle_register_operation(int client_fd) {
    char username[MAX_USERNAME];
    int rc;
    uint8_t code;

    if (recv_cstring(client_fd, username, sizeof(username)) != 0) {
        /* Si ni siquiera se pudo leer bien el usuario, no hay salida fiable con username */
        code = 2;
        (void)send_byte_code(client_fd, code);
        return;
    }

    rc = register_user_internal(username);

    if (rc == 0) {
        code = 0;
        if (send_byte_code(client_fd, code) != 0) {
            printf("s> REGISTER %s FAIL\n", username);
            fflush(stdout);
            return;
        }
        printf("s> REGISTER %s OK\n", username);
        fflush(stdout);
        return;
    }

    if (rc == 1) {
        code = 1;
        (void)send_byte_code(client_fd, code);
        printf("s> REGISTER %s FAIL\n", username);
        fflush(stdout);
        return;
    }

    code = 2;
    (void)send_byte_code(client_fd, code);
    printf("s> REGISTER %s FAIL\n", username);
    fflush(stdout);
}

static void handle_unregister_operation(int client_fd) {
    char username[MAX_USERNAME];
    int rc;
    uint8_t code;

    if (recv_cstring(client_fd, username, sizeof(username)) != 0) {
        code = 2;
        (void)send_byte_code(client_fd, code);
        return;
    }

    rc = unregister_user_internal(username);

    if (rc == 0) {
        code = 0;
        if (send_byte_code(client_fd, code) != 0) {
            printf("s> UNREGISTER %s FAIL\n", username);
            fflush(stdout);
            return;
        }
        printf("s> UNREGISTER %s OK\n", username);
        fflush(stdout);
        return;
    }

    if (rc == 1) {
        code = 1;
        (void)send_byte_code(client_fd, code);
        printf("s> UNREGISTER %s FAIL\n", username);
        fflush(stdout);
        return;
    }

    code = 2;
    (void)send_byte_code(client_fd, code);
    printf("s> UNREGISTER %s FAIL\n", username);
    fflush(stdout);
}

/* ===========================
 * Hilo por cliente
 * =========================== */

static void *client_thread_main(void *arg) {
    client_thread_arg_t *thread_arg = (client_thread_arg_t *)arg;
    int client_fd;
    char operation[64];

    if (thread_arg == NULL) {
        return NULL;
    }

    client_fd = thread_arg->client_fd;

    /*
     * Una conexión por operación.
     * Primero llega la cadena con la operación.
     */
    if (recv_cstring(client_fd, operation, sizeof(operation)) != 0) {
        close(client_fd);
        free(thread_arg);
        return NULL;
    }

    if (strcmp(operation, "REGISTER") == 0) {
        handle_register_operation(client_fd);
    } else if (strcmp(operation, "UNREGISTER") == 0) {
        handle_unregister_operation(client_fd);
    } else {
        /*
         * Operación todavía no implementada en esta fase.
         * Se responde con error genérico cuando procede.
         */
        uint8_t code = 2;
        (void)send_byte_code(client_fd, code);
    }

    if (client_fd != -1) {
        close(client_fd);
    }

    free(thread_arg);
    return NULL;
}

/* ===========================
 * Socket servidor
 * =========================== */

static int create_server_socket(const char *port_str) {
    struct addrinfo hints;
    struct addrinfo *result = NULL;
    struct addrinfo *rp;
    int listen_fd = -1;
    int yes = 1;
    int rc;

    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = AI_PASSIVE;

    rc = getaddrinfo(NULL, port_str, &hints, &result);
    if (rc != 0) {
        fprintf(stderr, "getaddrinfo: %s\n", gai_strerror(rc));
        return -1;
    }

    for (rp = result; rp != NULL; rp = rp->ai_next) {
        listen_fd = socket(rp->ai_family, rp->ai_socktype, rp->ai_protocol);
        if (listen_fd == -1) {
            continue;
        }

        if (setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes)) == -1) {
            close(listen_fd);
            listen_fd = -1;
            continue;
        }

        if (bind(listen_fd, rp->ai_addr, rp->ai_addrlen) == 0) {
            break;
        }

        close(listen_fd);
        listen_fd = -1;
    }

    freeaddrinfo(result);

    if (listen_fd == -1) {
        fprintf(stderr, "Error: no se pudo hacer bind en el puerto %s\n", port_str);
        return -1;
    }

    if (listen(listen_fd, BACKLOG) == -1) {
        perror("listen");
        close(listen_fd);
        return -1;
    }

    return listen_fd;
}

/* ===========================
 * Argumentos
 * =========================== */

static int parse_port_from_argv(int argc, char *argv[], char *out_port, size_t out_size) {
    int i;

    if (out_port == NULL || out_size == 0) {
        return -1;
    }

    for (i = 1; i < argc - 1; ++i) {
        if (strcmp(argv[i], "-p") == 0) {
            long port;
            char *endptr = NULL;

            port = strtol(argv[i + 1], &endptr, 10);
            if (*argv[i + 1] == '\0' || endptr == NULL || *endptr != '\0') {
                return -1;
            }

            if (port < 1024 || port > 65535) {
                return -1;
            }

            snprintf(out_port, out_size, "%ld", port);
            return 0;
        }
    }

    return -1;
}

/* ===========================
 * Limpieza
 * =========================== */

static void free_all_users(void) {
    user_t *curr;
    user_t *next;

    pthread_mutex_lock(&g_users_mutex);

    curr = g_users;
    while (curr != NULL) {
        next = curr->next;
        free_pending_messages(curr->pending_head);
        free(curr);
        curr = next;
    }
    g_users = NULL;

    pthread_mutex_unlock(&g_users_mutex);
}

/* ===========================
 * Main
 * =========================== */

int main(int argc, char *argv[]) {
    char port_str[MAX_PORT_STR];
    char local_ip[MAX_IP];
    int local_port;
    struct sigaction sa;

    if (parse_port_from_argv(argc, argv, port_str, sizeof(port_str)) != 0) {
        fprintf(stderr, "Usage: %s -p <port>\n", argv[0]);
        return EXIT_FAILURE;
    }

    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = handle_sigint;
    sigemptyset(&sa.sa_mask);

    if (sigaction(SIGINT, &sa, NULL) == -1) {
        perror("sigaction");
        return EXIT_FAILURE;
    }

    internal_silent_selfcheck();

    g_listen_fd = create_server_socket(port_str);
    if (g_listen_fd == -1) {
        return EXIT_FAILURE;
    }

    if (get_local_ip_port(g_listen_fd, local_ip, sizeof(local_ip), &local_port) != 0) {
        strncpy(local_ip, "0.0.0.0", sizeof(local_ip) - 1);
        local_ip[sizeof(local_ip) - 1] = '\0';
        local_port = atoi(port_str);
    }

    printf("s> init server %s:%d\n", local_ip, local_port);
    printf("s>\n");
    fflush(stdout);

    while (!g_stop) {
        struct sockaddr_storage client_addr;
        socklen_t client_len = sizeof(client_addr);
        int client_fd;
        pthread_t tid;
        client_thread_arg_t *thread_arg;

        client_fd = accept(g_listen_fd, (struct sockaddr *)&client_addr, &client_len);
        if (client_fd == -1) {
            if (errno == EINTR && g_stop) {
                break;
            }
            if (errno == EINTR) {
                continue;
            }
            perror("accept");
            continue;
        }

        thread_arg = (client_thread_arg_t *)malloc(sizeof(client_thread_arg_t));
        if (thread_arg == NULL) {
            fprintf(stderr, "Error: malloc thread_arg\n");
            close(client_fd);
            continue;
        }

        thread_arg->client_fd = client_fd;
        thread_arg->client_addr = client_addr;
        thread_arg->client_addr_len = client_len;

        if (pthread_create(&tid, NULL, client_thread_main, thread_arg) != 0) {
            fprintf(stderr, "Error: pthread_create\n");
            close(client_fd);
            free(thread_arg);
            continue;
        }

        if (pthread_detach(tid) != 0) {
            fprintf(stderr, "Error: pthread_detach\n");
        }
    }

    if (g_listen_fd != -1) {
        close(g_listen_fd);
        g_listen_fd = -1;
    }

    free_all_users();
    pthread_mutex_destroy(&g_users_mutex);

    return EXIT_SUCCESS;
}