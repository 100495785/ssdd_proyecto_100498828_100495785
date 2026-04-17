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
#define MAX_OPERATION_LEN 64
#define MAX_ID_STR 32

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

static int send_cstring(int fd, const char *str) {
    return send_all(fd, str, strlen(str) + 1);
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

static int connect_to_client_socket(const char *ip, const char *port_str) {
    struct addrinfo hints;
    struct addrinfo *result = NULL;
    struct addrinfo *rp;
    int fd = -1;
    int rc;

    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;

    rc = getaddrinfo(ip, port_str, &hints, &result);
    if (rc != 0) {
        return -1;
    }

    for (rp = result; rp != NULL; rp = rp->ai_next) {
        fd = socket(rp->ai_family, rp->ai_socktype, rp->ai_protocol);
        if (fd == -1) {
            continue;
        }

        if (connect(fd, rp->ai_addr, rp->ai_addrlen) == 0) {
            break;
        }

        close(fd);
        fd = -1;
    }

    freeaddrinfo(result);
    return fd;
}

static int send_uint_as_cstring(int fd, unsigned int value) {
    char buffer[MAX_ID_STR];
    snprintf(buffer, sizeof(buffer), "%u", value);
    return send_cstring(fd, buffer);
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

static void remove_pending_message_by_key(user_t *user,
                                          const char *sender,
                                          unsigned int id,
                                          const char *text) {
    pending_message_t *curr;
    pending_message_t *prev = NULL;

    if (user == NULL || sender == NULL || text == NULL) {
        return;
    }

    curr = user->pending_head;
    while (curr != NULL) {
        if (curr->id == id &&
            strcmp(curr->sender, sender) == 0 &&
            strcmp(curr->text, text) == 0) {

            if (prev == NULL) {
                user->pending_head = curr->next;
            } else {
                prev->next = curr->next;
            }

            if (user->pending_tail == curr) {
                user->pending_tail = prev;
            }

            free(curr);
            return;
        }

        prev = curr;
        curr = curr->next;
    }
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

/*
 * CONNECT:
 *  0 -> éxito
 *  1 -> no existe
 *  2 -> ya conectado
 * -1 -> error interno
 */
static int connect_user_internal(const char *username,
                                 const char *ip,
                                 const char *port) {
    user_t *user;

    if (username == NULL || ip == NULL || port == NULL) {
        return -1;
    }

    pthread_mutex_lock(&g_users_mutex);

    user = find_user_locked(username);
    if (user == NULL) {
        pthread_mutex_unlock(&g_users_mutex);
        return 1;
    }

    if (user->state == USER_CONNECTED) {
        pthread_mutex_unlock(&g_users_mutex);
        return 2;
    }

    strncpy(user->ip, ip, sizeof(user->ip) - 1);
    user->ip[sizeof(user->ip) - 1] = '\0';

    strncpy(user->port, port, sizeof(user->port) - 1);
    user->port[sizeof(user->port) - 1] = '\0';

    user->state = USER_CONNECTED;

    pthread_mutex_unlock(&g_users_mutex);
    return 0;
}

/*
 * DISCONNECT:
 *  0 -> éxito
 *  1 -> no existe
 *  2 -> no conectado
 * -1 -> error interno / IP no coincide
 */
static int disconnect_user_internal(const char *username, const char *request_ip) {
    user_t *user;

    if (username == NULL || request_ip == NULL) {
        return -1;
    }

    pthread_mutex_lock(&g_users_mutex);

    user = find_user_locked(username);
    if (user == NULL) {
        pthread_mutex_unlock(&g_users_mutex);
        return 1;
    }

    if (user->state != USER_CONNECTED) {
        pthread_mutex_unlock(&g_users_mutex);
        return 2;
    }

    if (strcmp(user->ip, request_ip) != 0) {
        pthread_mutex_unlock(&g_users_mutex);
        return -1;
    }

    user->state = USER_DISCONNECTED;
    user->ip[0] = '\0';
    user->port[0] = '\0';

    pthread_mutex_unlock(&g_users_mutex);
    return 0;
}

static void mark_user_disconnected_internal(const char *username) {
    user_t *user;

    if (username == NULL) {
        return;
    }

    pthread_mutex_lock(&g_users_mutex);

    user = find_user_locked(username);
    if (user != NULL) {
        user->state = USER_DISCONNECTED;
        user->ip[0] = '\0';
        user->port[0] = '\0';
    }

    pthread_mutex_unlock(&g_users_mutex);
}

/* ===========================
 * Envío servidor -> cliente
 * =========================== */

static int deliver_message_to_user(const char *ip,
                                   const char *port,
                                   const char *sender,
                                   unsigned int id,
                                   const char *text) {
    int fd;

    fd = connect_to_client_socket(ip, port);
    if (fd == -1) {
        return -1;
    }

    if (send_cstring(fd, "SEND MESSAGE") != 0 ||
        send_cstring(fd, sender) != 0 ||
        send_uint_as_cstring(fd, id) != 0 ||
        send_cstring(fd, text) != 0) {
        close(fd);
        return -1;
    }

    close(fd);
    return 0;
}

static int send_ack_to_sender(const char *ip,
                              const char *port,
                              unsigned int id) {
    int fd;

    fd = connect_to_client_socket(ip, port);
    if (fd == -1) {
        return -1;
    }

    if (send_cstring(fd, "SEND MESS ACK") != 0 ||
        send_uint_as_cstring(fd, id) != 0) {
        close(fd);
        return -1;
    }

    close(fd);
    return 0;
}

static void try_deliver_pending_for_user(const char *receiver_name) {
    while (1) {
        char receiver_ip[MAX_IP];
        char receiver_port[MAX_PORT_STR];
        char sender_name[MAX_USERNAME];
        char message_text[MAX_MESSAGE_LEN];
        unsigned int msg_id;

        bool sender_ack_connected = false;
        char sender_ack_ip[MAX_IP];
        char sender_ack_port[MAX_PORT_STR];

        user_t *receiver;
        user_t *sender;
        pending_message_t *msg;

        pthread_mutex_lock(&g_users_mutex);

        receiver = find_user_locked(receiver_name);
        if (receiver == NULL || receiver->state != USER_CONNECTED || receiver->pending_head == NULL) {
            pthread_mutex_unlock(&g_users_mutex);
            break;
        }

        msg = receiver->pending_head;

        strncpy(receiver_ip, receiver->ip, sizeof(receiver_ip) - 1);
        receiver_ip[sizeof(receiver_ip) - 1] = '\0';

        strncpy(receiver_port, receiver->port, sizeof(receiver_port) - 1);
        receiver_port[sizeof(receiver_port) - 1] = '\0';

        strncpy(sender_name, msg->sender, sizeof(sender_name) - 1);
        sender_name[sizeof(sender_name) - 1] = '\0';

        strncpy(message_text, msg->text, sizeof(message_text) - 1);
        message_text[sizeof(message_text) - 1] = '\0';

        msg_id = msg->id;

        sender = find_user_locked(sender_name);
        if (sender != NULL && sender->state == USER_CONNECTED) {
            sender_ack_connected = true;

            strncpy(sender_ack_ip, sender->ip, sizeof(sender_ack_ip) - 1);
            sender_ack_ip[sizeof(sender_ack_ip) - 1] = '\0';

            strncpy(sender_ack_port, sender->port, sizeof(sender_ack_port) - 1);
            sender_ack_port[sizeof(sender_ack_port) - 1] = '\0';
        }

        pthread_mutex_unlock(&g_users_mutex);

        if (deliver_message_to_user(receiver_ip, receiver_port, sender_name, msg_id, message_text) != 0) {
            mark_user_disconnected_internal(receiver_name);
            break;
        }

        pthread_mutex_lock(&g_users_mutex);

        receiver = find_user_locked(receiver_name);
        if (receiver != NULL) {
            remove_pending_message_by_key(receiver, sender_name, msg_id, message_text);
        }

        pthread_mutex_unlock(&g_users_mutex);

        printf("s> SEND MESSAGE %u FROM %s TO %s\n", msg_id, sender_name, receiver_name);
        fflush(stdout);

        if (sender_ack_connected) {
            (void)send_ack_to_sender(sender_ack_ip, sender_ack_port, msg_id);
        }
    }
}

/* ===========================
 * Operación SEND
 * =========================== */

static int handle_send_store(const char *sender_name,
                             const char *receiver_name,
                             const char *text,
                             unsigned int *assigned_id,
                             bool *receiver_connected,
                             char *receiver_ip,
                             size_t receiver_ip_size,
                             char *receiver_port,
                             size_t receiver_port_size,
                             bool *sender_ack_connected,
                             char *sender_ack_ip,
                             size_t sender_ack_ip_size,
                             char *sender_ack_port,
                             size_t sender_ack_port_size) {
    user_t *sender;
    user_t *receiver;
    unsigned int new_id;

    if (sender_name == NULL || receiver_name == NULL || text == NULL ||
        assigned_id == NULL || receiver_connected == NULL ||
        receiver_ip == NULL || receiver_port == NULL ||
        sender_ack_connected == NULL ||
        sender_ack_ip == NULL || sender_ack_port == NULL) {
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

    *receiver_connected = (receiver->state == USER_CONNECTED);
    if (*receiver_connected) {
        strncpy(receiver_ip, receiver->ip, receiver_ip_size - 1);
        receiver_ip[receiver_ip_size - 1] = '\0';

        strncpy(receiver_port, receiver->port, receiver_port_size - 1);
        receiver_port[receiver_port_size - 1] = '\0';
    } else {
        receiver_ip[0] = '\0';
        receiver_port[0] = '\0';
    }

    *sender_ack_connected = (sender->state == USER_CONNECTED);
    if (*sender_ack_connected) {
        strncpy(sender_ack_ip, sender->ip, sender_ack_ip_size - 1);
        sender_ack_ip[sender_ack_ip_size - 1] = '\0';

        strncpy(sender_ack_port, sender->port, sender_ack_port_size - 1);
        sender_ack_port[sender_ack_port_size - 1] = '\0';
    } else {
        sender_ack_ip[0] = '\0';
        sender_ack_port[0] = '\0';
    }

    pthread_mutex_unlock(&g_users_mutex);
    return 0;
}

/* ===========================
 * USERS
 * =========================== */

static int count_connected_users_locked(void) {
    int count = 0;
    user_t *curr = g_users;

    while (curr != NULL) {
        if (curr->state == USER_CONNECTED) {
            count++;
        }
        curr = curr->next;
    }

    return count;
}

static int fill_connected_users_locked(char ***out_names, int *out_count) {
    user_t *curr;
    char **names;
    int count;
    int i;

    if (out_names == NULL || out_count == NULL) {
        return -1;
    }

    count = count_connected_users_locked();
    *out_count = count;

    if (count == 0) {
        *out_names = NULL;
        return 0;
    }

    names = (char **)calloc((size_t)count, sizeof(char *));
    if (names == NULL) {
        return -1;
    }

    curr = g_users;
    i = 0;
    while (curr != NULL) {
        if (curr->state == USER_CONNECTED) {
            names[i] = (char *)malloc(strlen(curr->username) + 1);
            if (names[i] == NULL) {
                int j;
                for (j = 0; j < i; ++j) {
                    free(names[j]);
                }
                free(names);
                return -1;
            }
            strcpy(names[i], curr->username);
            i++;
        }
        curr = curr->next;
    }

    *out_names = names;
    return 0;
}

static void free_name_array(char **names, int count) {
    int i;

    if (names == NULL) {
        return;
    }

    for (i = 0; i < count; ++i) {
        free(names[i]);
    }
    free(names);
}

/* ===========================
 * Protocolo handlers
 * =========================== */

static void handle_register_operation(int client_fd) {
    char username[MAX_USERNAME];
    int rc;
    uint8_t code;

    if (recv_cstring(client_fd, username, sizeof(username)) != 0) {
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

static void handle_connect_operation(int client_fd,
                                     const struct sockaddr *client_sa) {
    char username[MAX_USERNAME];
    char port[MAX_PORT_STR];
    char ip[MAX_IP];
    int rc;
    uint8_t code;

    if (recv_cstring(client_fd, username, sizeof(username)) != 0 ||
        recv_cstring(client_fd, port, sizeof(port)) != 0) {
        code = 3;
        (void)send_byte_code(client_fd, code);
        return;
    }

    get_ip_str(client_sa, ip, sizeof(ip));
    rc = connect_user_internal(username, ip, port);

    if (rc == 0) {
        code = 0;
        if (send_byte_code(client_fd, code) != 0) {
            printf("s> CONNECT %s FAIL\n", username);
            fflush(stdout);
            return;
        }

        printf("s> CONNECT %s OK\n", username);
        fflush(stdout);

        try_deliver_pending_for_user(username);
        return;
    }

    code = (rc == 1) ? 1 : (rc == 2 ? 2 : 3);
    (void)send_byte_code(client_fd, code);

    printf("s> CONNECT %s FAIL\n", username);
    fflush(stdout);
}

static void handle_disconnect_operation(int client_fd,
                                        const struct sockaddr *client_sa) {
    char username[MAX_USERNAME];
    char ip[MAX_IP];
    int rc;
    uint8_t code;

    if (recv_cstring(client_fd, username, sizeof(username)) != 0) {
        code = 3;
        (void)send_byte_code(client_fd, code);
        return;
    }

    get_ip_str(client_sa, ip, sizeof(ip));
    rc = disconnect_user_internal(username, ip);

    if (rc == 0) {
        code = 0;
        (void)send_byte_code(client_fd, code);

        printf("s> DISCONNECT %s OK\n", username);
        fflush(stdout);
        return;
    }

    code = (rc == 1) ? 1 : (rc == 2 ? 2 : 3);
    (void)send_byte_code(client_fd, code);

    printf("s> DISCONNECT %s FAIL\n", username);
    fflush(stdout);
}

static void handle_send_operation(int client_fd) {
    char sender_name[MAX_USERNAME];
    char receiver_name[MAX_USERNAME];
    char text[MAX_MESSAGE_LEN];

    unsigned int assigned_id = 0;
    bool receiver_connected = false;
    char receiver_ip[MAX_IP];
    char receiver_port[MAX_PORT_STR];
    bool sender_ack_connected = false;
    char sender_ack_ip[MAX_IP];
    char sender_ack_port[MAX_PORT_STR];

    int rc;
    uint8_t code;

    if (recv_cstring(client_fd, sender_name, sizeof(sender_name)) != 0 ||
        recv_cstring(client_fd, receiver_name, sizeof(receiver_name)) != 0 ||
        recv_cstring(client_fd, text, sizeof(text)) != 0) {
        code = 2;
        (void)send_byte_code(client_fd, code);
        return;
    }

    rc = handle_send_store(sender_name,
                           receiver_name,
                           text,
                           &assigned_id,
                           &receiver_connected,
                           receiver_ip,
                           sizeof(receiver_ip),
                           receiver_port,
                           sizeof(receiver_port),
                           &sender_ack_connected,
                           sender_ack_ip,
                           sizeof(sender_ack_ip),
                           sender_ack_port,
                           sizeof(sender_ack_port));

    if (rc == 1) {
        code = 1;
        (void)send_byte_code(client_fd, code);
        printf("s> SEND MESSAGE %u FROM %s TO %s FAIL\n", assigned_id, sender_name, receiver_name);
        fflush(stdout);
        return;
    }

    if (rc != 0) {
        code = 2;
        (void)send_byte_code(client_fd, code);
        printf("s> SEND MESSAGE %u FROM %s TO %s FAIL\n", assigned_id, sender_name, receiver_name);
        fflush(stdout);
        return;
    }

    code = 0;
    if (send_byte_code(client_fd, code) != 0 ||
        send_uint_as_cstring(client_fd, assigned_id) != 0) {
        printf("s> SEND MESSAGE %u FROM %s TO %s FAIL\n", assigned_id, sender_name, receiver_name);
        fflush(stdout);
        return;
    }

    if (!receiver_connected) {
        printf("s> MESSAGE %u FROM %s TO %s STORED\n", assigned_id, sender_name, receiver_name);
        fflush(stdout);
        return;
    }

    if (deliver_message_to_user(receiver_ip, receiver_port, sender_name, assigned_id, text) != 0) {
        mark_user_disconnected_internal(receiver_name);
        printf("s> MESSAGE %u FROM %s TO %s STORED\n", assigned_id, sender_name, receiver_name);
        fflush(stdout);
        return;
    }

    pthread_mutex_lock(&g_users_mutex);
    {
        user_t *receiver = find_user_locked(receiver_name);
        if (receiver != NULL) {
            remove_pending_message_by_key(receiver, sender_name, assigned_id, text);
        }
    }
    pthread_mutex_unlock(&g_users_mutex);

    printf("s> SEND MESSAGE %u FROM %s TO %s\n", assigned_id, sender_name, receiver_name);
    fflush(stdout);

    if (sender_ack_connected) {
        (void)send_ack_to_sender(sender_ack_ip, sender_ack_port, assigned_id);
    }
}

static void handle_users_operation(int client_fd) {
    char username[MAX_USERNAME];
    user_t *requester;
    uint8_t code;
    char **names = NULL;
    int count = 0;
    int i;
    char count_str[32];

    /*
     * IMPORTANTE:
     * aquí asumimos que el cliente manda también el username
     * después de "USERS"
     */
    if (recv_cstring(client_fd, username, sizeof(username)) != 0) {
        code = 2;
        (void)send_byte_code(client_fd, code);
        return;
    }

    pthread_mutex_lock(&g_users_mutex);

    requester = find_user_locked(username);
    if (requester == NULL) {
        pthread_mutex_unlock(&g_users_mutex);
        code = 2;
        (void)send_byte_code(client_fd, code);
        printf("s> CONNECTEDUSERS FAIL\n");
        fflush(stdout);
        return;
    }

    if (requester->state != USER_CONNECTED) {
        pthread_mutex_unlock(&g_users_mutex);
        code = 1;
        (void)send_byte_code(client_fd, code);
        printf("s> CONNECTEDUSERS FAIL\n");
        fflush(stdout);
        return;
    }

    if (fill_connected_users_locked(&names, &count) != 0) {
        pthread_mutex_unlock(&g_users_mutex);
        code = 2;
        (void)send_byte_code(client_fd, code);
        printf("s> CONNECTEDUSERS FAIL\n");
        fflush(stdout);
        return;
    }

    pthread_mutex_unlock(&g_users_mutex);

    code = 0;
    if (send_byte_code(client_fd, code) != 0) {
        free_name_array(names, count);
        printf("s> CONNECTEDUSERS FAIL\n");
        fflush(stdout);
        return;
    }

    snprintf(count_str, sizeof(count_str), "%d", count);
    if (send_cstring(client_fd, count_str) != 0) {
        free_name_array(names, count);
        printf("s> CONNECTEDUSERS FAIL\n");
        fflush(stdout);
        return;
    }

    for (i = 0; i < count; ++i) {
        if (send_cstring(client_fd, names[i]) != 0) {
            free_name_array(names, count);
            printf("s> CONNECTEDUSERS FAIL\n");
            fflush(stdout);
            return;
        }
    }

    free_name_array(names, count);

    printf("s> CONNECTEDUSERS OK\n");
    fflush(stdout);
}

/* ===========================
 * Hilo por cliente
 * =========================== */

static void *client_thread_main(void *arg) {
    client_thread_arg_t *thread_arg = (client_thread_arg_t *)arg;
    int client_fd;
    char operation[MAX_OPERATION_LEN];

    if (thread_arg == NULL) {
        return NULL;
    }

    client_fd = thread_arg->client_fd;

    if (recv_cstring(client_fd, operation, sizeof(operation)) != 0) {
        close(client_fd);
        free(thread_arg);
        return NULL;
    }

    if (strcmp(operation, "REGISTER") == 0) {
        handle_register_operation(client_fd);
    } else if (strcmp(operation, "UNREGISTER") == 0) {
        handle_unregister_operation(client_fd);
    } else if (strcmp(operation, "CONNECT") == 0) {
        handle_connect_operation(client_fd, (struct sockaddr *)&thread_arg->client_addr);
    } else if (strcmp(operation, "DISCONNECT") == 0) {
        handle_disconnect_operation(client_fd, (struct sockaddr *)&thread_arg->client_addr);
    } else if (strcmp(operation, "SEND") == 0) {
        handle_send_operation(client_fd);
    } else if (strcmp(operation, "USERS") == 0) {
        handle_users_operation(client_fd);
    } else {
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