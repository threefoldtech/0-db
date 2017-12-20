#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <unistd.h>
#include <netdb.h>
#include <arpa/inet.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <netinet/in.h>
#include "rkv.h"
#include "index.h"
#include "data.h"

typedef enum resp_type_t {
    INTEGER,
    STRING,
    NIL,

} resp_type_t;

typedef struct resp_object_t {
    resp_type_t type;
    void *buffer;
    int length;

} resp_object_t;

typedef struct resp_request_t {
    int fd;
    int argc;
    resp_object_t **argv;

} resp_request_t;


static int yes = 1;

static int dispatcher(resp_request_t *request) {
    if(request->argv[0]->type != STRING) {
        printf("[+] not a string command, ignoring\n");
        return 0;
    }

    // PING
    if(!strncmp(request->argv[0]->buffer, "PING", request->argv[0]->length)) {
        printf("[+] redis: PING\n");
        send(request->fd, "+PONG\r\n", 7, 0);
        return 0;
    }

    // SET
    if(!strncmp(request->argv[0]->buffer, "SET", request->argv[0]->length)) {
        unsigned char hash[HASHSIZE];
        char *hashex;

        sha256_compute(hash, request->argv[2]->buffer, request->argv[2]->length);
        hashex = sha256_hex(hash);

        printf("[+] trying to insert entry\n");

        size_t offset = data_insert(request->argv[2]->buffer, hash, request->argv[2]->length);
        if(!index_entry_insert(hash, offset, request->argv[2]->length))
            printf("[+] key was already on the backend\n");

        // building response
        char response[HASHSIZE * 2 + 4];
        sprintf(response, "+%s\r\n", hashex);
        send(request->fd, response, strlen(response), 0);

        free(hashex);
        return 0;
    }

    // GET
    if(!strncmp(request->argv[0]->buffer, "GET", request->argv[0]->length)) {
        if(request->argv[1]->length != (HASHSIZE * 2)) {
            printf("[-] invalid key size\n");
            send(request->fd, "-Invalid key\r\n", 14, 0);
            return 1;
        }

        unsigned char hash[HASHSIZE];
        sha256_parse(request->argv[1]->buffer, hash);

        index_entry_t *entry;

        if(!(entry = index_entry_get(hash))) {
            printf("[-] key not found\n");
            send(request->fd, "$-1\r\n", 5, 0);
            return 1;
        }

        char strsize[64];
        size_t stroffset = sprintf(strsize, "%lu", entry->length);

        // $xx\r\n + payload + \r\n
        size_t total = 1 + stroffset + 2 + entry->length + 2;
        char *payload = data_get(entry->offset, entry->length);
        char *response = malloc(total);

        strcpy(response, "$");
        strcat(response, strsize);
        strcat(response, "\r\n");
        memcpy(response + stroffset + 3, payload, entry->length);
        memcpy(response + total - 2, "\r\n", 2);

        send(request->fd, response, total, 0);

        free(response);
        free(payload);

        return 0;
    }

    if(!strncmp(request->argv[0]->buffer, "STOP", request->argv[0]->length)) {
        send(request->fd, "+Stopping\r\n", 11, 0);
        return 2;
    }

    // unknown
    printf("unknown\n");
    send(request->fd, "-Command not supported\r\n", 24, 0);

    return 1;
}

char *mkobject(char *buffer, resp_object_t *argument) {
    if(*buffer == '$') {
        argument->type = STRING;
        argument->length = atoi(buffer + 1);
        printf("[+] string len: %d\n", argument->length);

        if(!(buffer = strchr(buffer, '\n')))
            return NULL;

        argument->buffer = buffer + 1;

        // buffer + length + \r\n
        return argument->buffer + argument->length + 2;
    }

    printf("[-] unknown type: %d, malformed\n", (char) *buffer);

    return NULL;
}

int resp(int fd) {
    resp_request_t command;

    char buffer[131072], *reader = NULL;
    int length;
    int dvalue = 0;

    command.fd = fd;

    while((length = recv(fd, buffer, sizeof(buffer) - 1, 0)) > 0) {
        buffer[length] = '\0';
        // printf("<%s>\n", buffer);

        // array
        if(buffer[0] != '*') {
            printf("[-] not an array\n");
            return 0;
        }

        command.argc = atoi(buffer + 1);
        printf("[+] resp: %d arguments\n", command.argc);

        if(!(reader = strchr(buffer, '\n')))
            return 0;

        reader += 1;
        command.argv = (resp_object_t **) malloc(sizeof(resp_type_t *) * command.argc);

        for(int i = 0; i < command.argc; i++) {
            command.argv[i] = malloc(sizeof(resp_object_t));
            if(!(reader = mkobject(reader, command.argv[i]))) {
                printf("argv error\n");
                return 0;
            }
        }

        dvalue = dispatcher(&command);

        for(int i = 0; i < command.argc; i++) {
            free(command.argv[i]);
        }

        free(command.argv);

        if(dvalue == 2)
            return 2;
    }

    return 0;
}

int redis_listen(char *listenaddr, int port) {
	int sockfd, cfd;
	struct sockaddr_in addr_listen, addr_client;
	socklen_t addr_client_len;
	char *client_ip;

	addr_listen.sin_family = AF_INET;
	addr_listen.sin_port = htons(port);
	addr_listen.sin_addr.s_addr = inet_addr(listenaddr);

	addr_client_len = sizeof(addr_client);

	if((sockfd = socket(AF_INET, SOCK_STREAM, 0)) == -1)
		diep("[-] socket");

	if(setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(int)) == -1)
		diep("[-] setsockopt");

	if(bind(sockfd, (struct sockaddr*) &addr_listen, sizeof(addr_listen)) == -1)
		diep("[-] bind");

	if(listen(sockfd, 32) == -1)
		diep("[-] listen");

	while(1) {
		printf("[+] waiting new connection...\n");

		if((cfd = accept(sockfd, (struct sockaddr *)&addr_client, &addr_client_len)) == -1)
			perror("[-] accept");

		client_ip = inet_ntoa(addr_client.sin_addr);
		printf("[+] connection from %s\n", client_ip);

        int ctrl = resp(cfd);
        close(cfd);

        if(ctrl == 2) {
            printf("[+] stopping daemon\n");
            close(sockfd);
            return 1;
        }
	}

	return 0;
}
