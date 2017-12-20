#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <stdint.h>
#include <errno.h>
#include "rkv.h"
#include "redis.h"
#include "index.h"
#include "data.h"

void warnp(char *str) {
	fprintf(stderr, "[-] %s: %s\n", str, strerror(errno));
}

void diep(char *str) {
    warnp(str);
	exit(EXIT_FAILURE);
}

int main(void) {
    uint16_t indexid = index_init();
    data_init(indexid);

    redis_listen(LISTEN_ADDR, LISTEN_PORT);

    index_destroy();
    data_destroy();

	return 0;
}
