#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <stdint.h>
#include "rkv.h"
#include "redis.h"
#include "index.h"
#include "data.h"

void diep(char *str) {
	perror(str);
	exit(EXIT_FAILURE);
}

int main(void) {
    index_init();
    data_init();
    redis_listen(LISTEN_ADDR, LISTEN_PORT);

    index_destroy();
    data_destroy();

	return 0;
}
