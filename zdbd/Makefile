EXEC = zdb
SRC = $(wildcard *.c)
OBJ = $(SRC:.c=.o)

CFLAGS += -g -std=gnu99 -O0 -W -Wall -Wextra -Wno-implicit-fallthrough -I../libzdb
LDFLAGS += -rdynamic ../libzdb/libzdb.a

MACHINE := $(shell uname -m)
ifeq ($(MACHINE),x86_64)
	# explicit sse4.2 flags
	CFLAGS += -msse4.2
endif

# grab version from git, if possible
REVISION := $(shell git describe --abbrev=8 --dirty --always --tags)
ifneq ($(REVISION),)
	CFLAGS += -DZDBD_REVISION=\"$(REVISION)\"
endif

ifeq ($(STATIC),1)
	LDFLAGS += -static
endif

ifeq ($(COVERAGE),1)
	CFLAGS += -pg -coverage -fprofile-arcs -ftest-coverage
	LDFLAGS += -lgcov --coverage
endif

ifeq ($(PROFILE),1)
	CFLAGS += -pg
	LDFLAGS += -pg
endif

ifeq ($(PREFIX),)
	PREFIX := /usr/local
endif

all: $(EXEC)

release: CFLAGS += -DRELEASE -O2
release: clean $(EXEC)

$(EXEC): $(OBJ)
	$(CC) -o $@ $^ $(LDFLAGS)

%.o: %.c
	$(CC) $(CFLAGS) -c $<

clean:
	$(RM) *.o

mrproper: clean
	$(RM) $(EXEC)
	$(RM) *.gcno *.gcda *.gcov

install: $(EXEC)
	install -d $(DESTDIR)$(PREFIX)/bin
	install -m 755 $(EXEC) $(DESTDIR)$(PREFIX)/bin/
