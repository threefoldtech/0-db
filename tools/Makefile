SRC=$(wildcard *.c)
OBJ=$(SRC:.c=.o)

CFLAGS=-g -std=gnu99 -W -Wall -O2 -msse4.2 -I../src
LDFLAGS=-rdynamic 

all: integrity-check index-dump

release: CFLAGS+=-DRELEASE
release: $(EXEC)

integrity-check: integrity-check.o
	$(CC) -o $@ $^ $(LDFLAGS)

index-dump: index-dump.o
	$(CC) -o $@ $^ $(LDFLAGS)

%.o: %.c
	$(CC) $(CFLAGS) -c $<

clean:
	$(RM) *.o

mrproper: clean
	$(RM) integrity-check index-dump
