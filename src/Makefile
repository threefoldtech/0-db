EXEC=zdb
SRC=$(wildcard *.c)
OBJ=$(SRC:.c=.o)

CFLAGS=-g -W -Wall -O2 -msse4.2 -Wno-implicit-fallthrough
LDFLAGS=-rdynamic

# grab version from git, if possible
REVISION := $(shell git describe --abbrev=8 --dirty --always --tags)
ifneq ($(REVISION),)
	CFLAGS+=-DREVISION=\"$(REVISION)\"
endif

all: $(EXEC)

release: CFLAGS+=-DRELEASE
release: clean $(EXEC)

static: LDFLAGS+=-static
static: clean $(EXEC)

$(EXEC): $(OBJ)
	$(CC) -o $@ $^ $(LDFLAGS)

%.o: %.c
	$(CC) $(CFLAGS) -c $<

clean:
	$(RM) *.o

mrproper: clean
	$(RM) $(EXEC)
