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
#include "libzdb.h"
#include "zdbd.h"
#include "redis.h"

#define MAXEVENTS 64
#define EVTIMEOUT 150
struct kevent evset;

static int socket_client_accept(redis_handler_t *redis, int fd) {
    int clientfd;

    if((clientfd = accept(fd, NULL, NULL)) == -1) {
        zdbd_warnp("accept");
        return 1;
    }

    socket_nonblock(clientfd);
    socket_keepalive(clientfd);
    socket_client_new(clientfd);

    zdbd_verbose("[+] incoming connection (socket %d)\n", clientfd);

    EV_SET(&evset, clientfd, EVFILT_READ, EV_ADD, 0, 0, NULL);
    if(kevent(redis->evfd, &evset, 1, NULL, 0, NULL) == -1) {
        zdbd_warnp("kevent: filter read");
        return 1;
    }

    EV_SET(&evset, clientfd, EVFILT_WRITE, EV_ADD | EV_ONESHOT, 0, 0, NULL);
    if(kevent(redis->evfd, &evset, 1, NULL, 0, NULL) == -1) {
        zdbd_warnp("kevent: filter write");
        return 1;
    }

    return 1;
}

static int socket_event(struct kevent *events, int notified, redis_handler_t *redis) {
    struct kevent *ev;

    for(int i = 0; i < notified; i++) {
        int newclient = 0;
        ev = events + i;

        if(ev->flags & EV_EOF) {
            EV_SET(&evset, ev->ident, EVFILT_READ, EV_DELETE, 0, 0, NULL);

            if(kevent(redis->evfd, &evset, 1, NULL, 0, NULL) == -1)
                zdbd_diep("kevent");

            socket_client_free(ev->ident);
            continue;

        }

        // main socket event: we have a new client
        // creating the new client and accepting it
        for(int i = 0; i < redis->fdlen; i++) {
            if((int) ev->ident == redis->mainfd[i]) {
                socket_client_accept(redis, (int) ev->ident);
                newclient = 1;
            }
        }

        // we don't need to proceed more if it's
        // a new client connection
        if(newclient)
            continue;

        if(ev->filter == EVFILT_READ) {
            // call the redis chunk event handler
            resp_status_t ctrl = redis_chunk_read(ev->ident);

            // client error, we discard it
            if(ctrl == RESP_STATUS_DISCARD || ctrl == RESP_STATUS_DISCONNECTED) {
                socket_client_free(ev->ident);
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

        if(ev->filter == EVFILT_WRITE) {
            redis_delayed_write(ev->ident);
        }
    }

    return 0;
}

int socket_handler(redis_handler_t *handler) {
    zdbd_stats_t *dstats = &zdbd_rootsettings.stats;
    struct kevent evlist[MAXEVENTS];
    struct timespec timeout = {
        .tv_sec = 0,
        .tv_nsec = EVTIMEOUT * 1000000
    };


    if((handler->evfd = kqueue()) < 0)
        zdbd_diep("kqueue");

    for(int i = 0; i < handler->fdlen; i++) {
        // initialize an empty struct
        EV_SET(&evset, handler->mainfd[i], EVFILT_READ, EV_ADD, 0, 0, NULL);

        if(kevent(handler->evfd, &evset, 1, NULL, 0, NULL) == -1)
            zdbd_diep("kevent");
    }

    // wait for clients
    // this is how we support multi-client using a single thread
    // note that, we will only handle one request at a time
    // allows multiple clients to be connected

    while(1) {
        int n = kevent(handler->evfd, NULL, 0, evlist, MAXEVENTS, &timeout);
        dstats->netevents += 1;

        if(n == 0) {
            // timeout reached, checking for background
            // or pending recurring task to do
            redis_idle_process();
            continue;
        }

        if(socket_event(evlist, n, handler) == 1)
            return 1;

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

#endif // __APPLE__
