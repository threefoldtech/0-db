#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netinet/in.h>

// this implementation is only used on linux
#ifdef __linux__

#include <sys/epoll.h>
#include "libzdb.h"
#include "zdbd.h"
#include "redis.h"

#define MAXEVENTS 64
#define EVTIMEOUT 200

static int socket_client_accept(redis_handler_t *redis, int fd) {
    int clientfd;

    if((clientfd = accept(fd, NULL, NULL)) == -1) {
        zdbd_verbosep("socket_event", "accept");
        return 0;
    }

    socket_nonblock(clientfd);
    socket_keepalive(clientfd);
    socket_client_new(clientfd);

    zdbd_verbose("[+] incoming connection (socket %d)\n", clientfd);

    // add client to the epoll list
    struct epoll_event event;

    memset(&event, 0, sizeof(struct epoll_event));

    event.data.fd = clientfd;
    event.events = EPOLLIN | EPOLLOUT | EPOLLET;

    // we use edge-level because of how the
    // upload works (need to be notified when client
    // is ready to receive data, only one time)

    if(epoll_ctl(redis->evfd, EPOLL_CTL_ADD, clientfd, &event) < 0) {
        zdbd_verbosep("socket_event", "epoll_ctl");
        return 0;
    }

    return 1;
}

static int socket_event(struct epoll_event *events, int notified, redis_handler_t *redis) {
    struct epoll_event *ev;

    for(int i = 0; i < notified; i++) {
        int newclient = 0;
        ev = events + i;

        // epoll issue
        // discard this client
        if((ev->events & EPOLLERR) || (ev->events & EPOLLHUP)) {
            zdbd_verbosep("socket_event", "epoll");
            socket_client_free(ev->data.fd);
            continue;
        }

        // main socket event: we have a new client
        // create the new client and accept it
        for(int i = 0; i < redis->fdlen; i++) {
            if(ev->data.fd == redis->mainfd[i]) {
                socket_client_accept(redis, ev->data.fd);
                newclient = 1;
            }
        }

        // we don't need to proceed more if it's
        // a new client connection
        if(newclient)
            continue;

        // data available for reading
        // let's read what's available and check
        // the response code
        if(ev->events & EPOLLIN) {
            // call the redis chunk event handler
            resp_status_t ctrl = redis_chunk_read(ev->data.fd);

            // client error, we discard it
            if(ctrl == RESP_STATUS_DISCARD || ctrl == RESP_STATUS_DISCONNECTED) {
                socket_client_free(ev->data.fd);
                continue;
            }

            // (dirty) way the STOP event is handled
            if(ctrl == RESP_STATUS_SHUTDOWN) {
                zdb_log("[+] stopping daemon\n");

                for(int i = 0; i < redis->fdlen; i++)
                    close(redis->mainfd[i]);

                return 1;
            }
        }

        // client is ready for writing, let's check if any
        // data still needs to be sent or not
        if(ev->events & EPOLLOUT) {
            redis_delayed_write(ev->data.fd);
        }
    }

    return 0;
}

int socket_handler(redis_handler_t *handler) {
    struct epoll_event event;
    struct epoll_event *events = NULL;
    zdbd_stats_t *dstats = &zdbd_rootsettings.stats;

    // initialize empty struct
    memset(&event, 0, sizeof(struct epoll_event));

    if((handler->evfd = epoll_create1(0)) < 0)
        zdbd_diep("epoll_create1");


    for(int i = 0; i < handler->fdlen; i++) {
        event.data.fd = handler->mainfd[i];
        event.events = EPOLLIN;

        if(epoll_ctl(handler->evfd, EPOLL_CTL_ADD, handler->mainfd[i], &event) < 0)
            zdbd_diep("epoll_ctl");
    }

    events = calloc(MAXEVENTS, sizeof event);

    // wait for clients
    // this is how we support multi-client using a single thread
    // note that, we will only handle one request at a time
    // allows multiple clients to be connected

    while(1) {
        int n = epoll_wait(handler->evfd, events, MAXEVENTS, EVTIMEOUT);
        dstats->netevents += 1;

        if(n == 0) {
            // timeout reached, checking for background
            // or pending recurring task to do
            redis_idle_process();
            continue;
        }

        if(socket_event(events, n, handler) == 1) {
            free(events);
            return 1;
        }

        // force idle process trigger after fixed amount
        // of commands, otherwise spamming the server enough
        // would never trigger it
        if(dstats->netevents % 100 == 0) {
            zdbd_debug("[+] sockets: forcing idle process [%lu]\n", dstats->netevents);
            redis_idle_process();
        }
    }

    return 0;
}

#endif // __linux__
