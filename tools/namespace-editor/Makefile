EXEC = namespace-editor
SRC = $(wildcard *.c)
OBJ = $(SRC:.c=.o)

CFLAGS += -g -std=gnu99 -W -Wall -O2 -I../../libzdb
LDFLAGS += ../../libzdb/libzdb.a -rdynamic 

MACHINE := $(shell uname -m)
ifeq ($(MACHINE),x86_64)
	# explicit sse4.2 flags
	CFLAGS += -msse4.2
endif

ifeq ($(COVERAGE),1)
	CFLAGS += -coverage -fprofile-arcs -ftest-coverage
	LDFLAGS += -lgcov --coverage
endif

all: $(EXEC)

release: CFLAGS += -DRELEASE
release: $(EXEC)

$(EXEC): $(OBJ)
	$(CC) -o $@ $^ $(LDFLAGS)

%.o: %.c
	$(CC) $(CFLAGS) -c $<

clean:
	$(RM) *.o

mrproper: clean
	$(RM) $(EXEC)
