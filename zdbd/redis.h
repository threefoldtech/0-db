#ifndef __ZDB_REDIS_H
    #define __ZDB_REDIS_H

    #include <sys/time.h>

    // redis_hardsend is a macro that allows us to send
    // easily a hardcoded message to the client, without the need to
    // specify the size, but keep size computed at compile time
    //
    // this is useful when you want to write a message to the client without
    // writing yourself the size of the string. please only use this with strings
    //
    // sizeof(message) will end with the null character, to appended with \r\n
    // the size will just be +1
    #define redis_hardsend(client, message) redis_reply_stack(client, message "\r\n", sizeof(message) + 1)

    //
    // redis protocol oriented objects
    //
    typedef enum resp_type_t {
        INTEGER,
        STRING,
        NIL,

    } resp_type_t;

    typedef struct resp_object_t {
        resp_type_t type;
        void *buffer;
        int length;
        int filled;
        int size;

    } resp_object_t;

    typedef enum resp_state_t {
        RESP_EMPTY,
        RESP_FILLIN_HEADER,
        RESP_FILLIN_PAYLOAD,

    } resp_state_t;

    // represents one redis command with arguments
    typedef struct resp_request_t {
        resp_state_t state;     // current fill-in status
        int fillin;             // reminder of progress
        int argc;               // arguments count
        resp_object_t **argv;   // list of arguments
        uint32_t owner;         // source owner id

        // source owner id is used mainly for replication
        //
        // by default, the owner is the zdb id itself (the zdb
        // which generated the request), but when using mirroring
        // this owner id can be replaced by original owner id (the zdb
        // which generated the original message and started mirroring it)
        //
        // this id is used to stop propagating replication in a full mesh
        // zdb topology (and basically master-master replication)

    } resp_request_t;

    typedef enum resp_status_t {
        RESP_STATUS_SUCCESS,
        RESP_STATUS_ABNORMAL,
        RESP_STATUS_DISCARD,
        RESP_STATUS_DISCONNECTED,
        RESP_STATUS_CONTINUE,
        RESP_STATUS_DONE,
        RESP_STATUS_SHUTDOWN,
        RESP_STATUS_RESET,

    } resp_status_t;

    // tracking each client in memory
    // we need to create an object per client to keep
    // some session-live persistent data, such as the namespace
    // attached to a socket, and so on

    typedef struct buffer_t {
        char *buffer;
        size_t length;
        size_t remain;
        char *reader;
        char *writer;

    } buffer_t;

    typedef struct redis_response_t {
        void *buffer;  // begin of the buffer that will be freed
        void *reader;  // current pointer to the buffer, for the next chunk to be sent
        size_t length; // length of the remaining payload to send

        // pointer to a desctuctor function that will be
        // called if the send if fully completes, to clean up
        // the buffer
        void (*destructor)(void *target);

        struct redis_response_t *next;

    } redis_response_t;

    typedef struct command_t command_t;
    typedef struct redis_client_t redis_client_t;

    // command name and associated handler
    struct command_t {
        char *command;
        int (*handler)(redis_client_t *client);

    };

    // represents one client in memory
    struct redis_client_t {
        int fd;           // socket file descriptor
        namespace_t *ns;  // connection namespace
        time_t connected; // connection time
        size_t commands;  // request (commands) counter
        int writable;     // can the client write to the namespace
        int admin;        // is the client is admin
        int mirror;       // does this client needs a mirroring
        int master;       // does this client is a 'master' (forwarder)
        char *nonce;      // nonce challenge used by secure auth
        buffer_t buffer;  // per-client buffer

        // each client can request to wait for an event
        // an event is basicly somebody else doing some command
        // we keep track if a client wants to monitor some event
        // and a pointer to the last command executed
        size_t watchtimeout;          // watch command timeout
        struct timeval watchtime;     // tracker for timeout
        command_t *watching;
        command_t *executed;

        // each client will be attached to a request
        // this request will contain one-per-one commands
        resp_request_t *request;

        // each client can have some (optional) pending
        // writes, we attach a delayed async writer per
        // client
        redis_response_t *responses;
        redis_response_t *responsetail;
    };

    // represents all clients in memory
    typedef struct redis_clients_t {
        size_t length;
        redis_client_t **list;

    } redis_clients_t;

    // minimum (default) amount of clients pre-allocated
    #define REDIS_CLIENTS_INITIAL_LENGTH 32

    // per-client buffer
    #define REDIS_BUFFER_SIZE 8192

    // maximum payload size
    #define REDIS_MAX_PAYLOAD 8 * 1024 * 1024

    typedef struct redis_handler_t {
        int *mainfd;  // main sockets handler (support multiple sockets)
        int fdlen;    // amount of sockets on the list
        int evfd;     // event handler (epoll, kqueue, ...)

    } redis_handler_t;

    typedef struct redis_bulk_t {
        size_t length;
        size_t writer;
        unsigned char *buffer;

    } redis_bulk_t;

    int redis_listen(char *listenaddr, char *port, char *socket);
    resp_status_t redis_chunk_read(int fd);
    resp_status_t redis_delayed_write(int fd);

    void socket_nonblock(int fd);
    void socket_keepalive(int fd);
    void socket_block(int fd);

    // wait command helpers
    void redis_client_set_watcher(redis_client_t *client, command_t *handler, size_t timeoutms);
    void redis_client_unset_watcher(redis_client_t *client);

    void redis_bulk_append(redis_bulk_t *bulk, void *data, size_t length);
    redis_bulk_t redis_bulk(void *payload, size_t length);

    // abstract handler implemented by platform dependent
    // code (see socket_epoll, socket_kqueue, ...)
    int socket_handler(redis_handler_t *handler);

    // managing clients
    redis_client_t *socket_client_new(int fd);
    void socket_client_free(int fd);
    int redis_detach_clients(namespace_t *namespace);

    // socket generic reply
    redis_response_t *redis_response_new(void *payload, size_t length, void (*destructor)(void *));
    int redis_reply_heap(redis_client_t *client, void *payload, size_t length, void (*destructor)(void *));
    int redis_reply_stack(redis_client_t *client, void *payload, size_t length);

    int redis_posthandler_client(redis_client_t *client);
    void redis_idle_process();
#endif
