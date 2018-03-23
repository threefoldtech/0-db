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
        if((ev->events & EPOLLERR) || (ev->events & EPOLLHUP)) {
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
            event.events = EPOLLIN | EPOLLOUT | EPOLLET;

            if(epoll_ctl(redis->evfd, EPOLL_CTL_ADD, clientfd, &event) < 0) {
                warnp("epoll_ctl");
                continue;
            }

            continue;
        }

        // data available for reading
        // let's read what'a available and checking
        // the response code
        if(ev->events & EPOLLIN) {
            // calling the redis chunk event handler
            resp_status_t ctrl = redis_chunk_read(ev->data.fd);

            // client error, we discard it
            if(ctrl == RESP_STATUS_DISCARD || ctrl == RESP_STATUS_DISCONNECTED) {
                socket_client_free(ev->data.fd);
                continue;
            }

            // (dirty) way the STOP event is handled
            if(ctrl == RESP_STATUS_SHUTDOWN) {
                printf("[+] stopping daemon\n");
                close(redis->mainfd);
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
