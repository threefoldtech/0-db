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
#include "zerodb.h"
#include "index.h"
#include "data.h"
#include "namespace.h"
#include "redis.h"

#define MAXEVENTS 64

static int socket_event(struct epoll_event *events, int notified, redis_handler_t *redis) {
    struct epoll_event *ev;

    for(int i = 0; i < notified; i++) {
        ev = events + i;

        // epoll issue
        // discarding this client
        if((ev->events & EPOLLERR) || (ev->events & EPOLLHUP) || (!(ev->events & EPOLLIN))) {
            warnp("epoll");
            socket_client_free(ev->data.fd);
            continue;
        }

        // main socket event: we have a new client
        // creating the new client and accepting it
        if(ev->data.fd == redis->mainfd) {
            int clientfd;

            if((clientfd = accept(redis->mainfd, NULL, NULL)) == -1) {
                warnp("accept");
                continue;
            }

            socket_nonblock(clientfd);
            socket_client_new(clientfd);

            verbose("[+] incoming connection (socket %d)\n", clientfd);

            // adding client to the epoll list
            struct epoll_event event;

            memset(&event, 0, sizeof(struct epoll_event));

            event.data.fd = clientfd;
            event.events = EPOLLIN;

            if(epoll_ctl(redis->evfd, EPOLL_CTL_ADD, clientfd, &event) < 0) {
                warnp("epoll_ctl");
                continue;
            }

            continue;
        }

        // here is the "blocking" state
        // which only allows us to parse one client at a time
        // we will only proceed a full request at a time
        //
        // -- FIXME --
        // basicly here is one security issue, a client could
        // potentially lock the daemon by starting a command and not complete it
        //
        socket_block(ev->data.fd);

        // dispatching client event
        int ctrl = redis_response(ev->data.fd);

        // client error, we discard it
        if(ctrl == 3) {
            socket_client_free(ev->data.fd);
            continue;
        }

        socket_nonblock(ev->data.fd);

        // dirty way the STOP event is handled
        if(ctrl == 2) {
            printf("[+] stopping daemon\n");
            close(redis->mainfd);
            return 1;
        }
    }

    return 0;
}

int socket_handler(redis_handler_t *handler) {
    struct epoll_event event;
    struct epoll_event *events = NULL;

    // initialize empty struct
    memset(&event, 0, sizeof(struct epoll_event));

    if((handler->evfd = epoll_create1(0)) < 0)
        diep("epoll_create1");

    event.data.fd = handler->mainfd;
    event.events = EPOLLIN;

    if(epoll_ctl(handler->evfd, EPOLL_CTL_ADD, handler->mainfd, &event) < 0)
        diep("epoll_ctl");

    events = calloc(MAXEVENTS, sizeof event);

    // waiting for clients
    // this is how we supports multi-client using a single thread
    // note that, we will only proceed one request at a time
    // allows multiple client to be connected

    while(1) {
        int n = epoll_wait(handler->evfd, events, MAXEVENTS, -1);
        if(socket_event(events, n, handler) == 1) {
            free(events);
            return 1;
        }
    }

    return 0;
}

#endif // __linux__
