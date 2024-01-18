LIB = libzdb
SRC = $(wildcard *.c)
OBJ = $(SRC:.c=.o)

CFLAGS += -g -fPIC -std=gnu11 -O0 -W -Wall -Wextra -Wno-implicit-fallthrough
LDFLAGS += -rdynamic

MACHINE := $(shell uname -m)
ifeq ($(MACHINE),x86_64)
	# explicit sse4.2 flags
	CFLAGS += -msse4.2
endif

# grab version from git, if possible
REVISION := $(shell git describe --abbrev=8 --dirty --always --tags)
ifeq ($(REVISION),)
	REVISION := $(shell grep ZDB_VERSION libzdb.h | awk '{ print $$3 }' | sed s/'"'//g)
endif

# add revision to build
CFLAGS += -DZDB_REVISION=\"$(REVISION)\"

ifeq ($(STATIC),1)
	LDFLAGS += -static
endif

ifeq ($(COVERAGE),1)
	CFLAGS += -coverage -fprofile-arcs -ftest-coverage
	LDFLAGS += -lgcov --coverage
endif

ifeq ($(PROFILE),1)
	CFLAGS += -pg
	LDFLAGS += -pg
endif

all: $(LIB).a $(LIB).so

release: CFLAGS += -DRELEASE -O2
release: clean $(LIB).a $(LIB).so

$(LIB).so: $(OBJ)
	$(CC) -shared -o $@ $^ $(LDFLAGS)

$(LIB).a: $(OBJ)
	ar rcs $(LIB).a $(OBJ)

%.o: %.c
	$(CC) $(CFLAGS) -c $<

clean:
	$(RM) *.o

mrproper: clean
	$(RM) $(LIB).a $(LIB).so
	$(RM) *.gcno *.gcda *.gcov
