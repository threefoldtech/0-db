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
#include "redis.h"
#include "zerodb.h"

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

            close(ev->ident);

            continue;

        } else if((int) ev->ident == redis->mainfd) {
            int clientfd;
            char *clientip;
            struct sockaddr_in addr_client;
            socklen_t addr_client_len;

            addr_client_len = sizeof(addr_client);

            if((clientfd = accept(redis->mainfd, (struct sockaddr *)&addr_client, &addr_client_len)) == -1)
                warnp("accept");

            socket_nonblock(clientfd);

            clientip = inet_ntoa(addr_client.sin_addr);
            verbose("[+] incoming connection from %s (socket %d)\n", clientip, clientfd);

            EV_SET(&evset, clientfd, EVFILT_READ, EV_ADD, 0, 0, NULL);
            if(kevent(redis->evfd, &evset, 1, NULL, 0, NULL) == -1)
                diep("kevent");

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
        socket_block(ev->ident);

        // dispatching client event
        int ctrl = redis_response(ev->ident);

        // client error, we discard it
        if(ctrl == 3) {
            close(ev->ident);
            continue;
        }

        socket_nonblock(ev->ident);

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
