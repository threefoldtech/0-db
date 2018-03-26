#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netinet/in.h>

// this implementation is used on macos and freebsd
#ifdef __APPLE__

#include <sys/event.h>
#include "zerodb.h"
#include "index.h"
#include "data.h"
#include "namespace.h"
#include "redis.h"

#define MAXEVENTS 64
struct kevent evset;

static int socket_event(struct kevent *events, int notified, redis_handler_t *redis) {
    struct kevent *ev;

    for(int i = 0; i < notified; i++) {
        ev = events + i;

        if(ev->flags & EV_EOF) {
            EV_SET(&evset, ev->ident, EVFILT_READ, EV_DELETE, 0, 0, NULL);

            if(kevent(redis->evfd, &evset, 1, NULL, 0, NULL) == -1)
                diep("kevent");

            socket_client_free(ev->ident);
            continue;

        } else if((int) ev->ident == redis->mainfd) {
            int clientfd;

            if((clientfd = accept(redis->mainfd, NULL, NULL)) == -1) {
                warnp("accept");
                continue;
            }

            socket_nonblock(clientfd);
            socket_client_new(clientfd);

            verbose("[+] incoming connection (socket %d)\n", clientfd);


            EV_SET(&evset, clientfd, EVFILT_READ, EV_ADD, 0, 0, NULL);
            if(kevent(redis->evfd, &evset, 1, NULL, 0, NULL) == -1) {
                warnp("kevent: filter read");
                continue;
            }

            EV_SET(&evset, clientfd, EVFILT_WRITE, EV_ADD, 0, 0, NULL);
            if(kevent(redis->evfd, &evset, 1, NULL, 0, NULL) == -1) {
                warnp("kevent: filter write");
                continue;
            }

            continue;
        }

        if(ev->filter == EVFILT_READ) {
            // calling the redis chunk event handler
            resp_status_t ctrl = redis_chunk_read(ev->ident);

            // client error, we discard it
            if(ctrl == RESP_STATUS_DISCARD || ctrl == RESP_STATUS_DISCONNECTED) {
                socket_client_free(ev->ident);
                continue;
            }

            // (dirty) way the STOP event is handled
            if(ctrl == RESP_STATUS_SHUTDOWN) {
                printf("[+] stopping daemon\n");
                close(redis->mainfd);
                return 1;
            }
        }

        if(ev->filter == EVFILT_WRITE) {
            redis_delayed_write(ev->ident);
        }
    }

    return 0;
}

int socket_handler(redis_handler_t *handler) {
    struct kevent evlist[MAXEVENTS];

    // initialize empty struct
    EV_SET(&evset, handler->mainfd, EVFILT_READ, EV_ADD, 0, 0, NULL);

    if((handler->evfd = kqueue()) < 0)
        diep("kqueue");

    if(kevent(handler->evfd, &evset, 1, NULL, 0, NULL) == -1)
        diep("kevent");

    // waiting for clients
    // this is how we supports multi-client using a single thread
    // note that, we will only proceed one request at a time
    // allows multiple client to be connected

    while(1) {
        int n = kevent(handler->evfd, NULL, 0, evlist, MAXEVENTS, NULL);
        if(socket_event(evlist, n, handler) == 1)
            return 1;
    }

    return 0;
}

#endif // __APPLE__
