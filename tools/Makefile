EXEC=integrity-check
SRC=$(wildcard *.c)
OBJ=$(SRC:.c=.o)

CFLAGS=-g -W -Wall -O2 -msse4.2 -I../src
LDFLAGS=-rdynamic 

all: $(EXEC)

release: CFLAGS+=-DRELEASE
release: $(EXEC)

integrity-check: integrity-check.o
	$(CC) -o $@ $^ $(LDFLAGS)

%.o: %.c
	$(CC) $(CFLAGS) -c $<

clean:
	$(RM) *.o

mrproper: clean
	$(RM) integrity-check
