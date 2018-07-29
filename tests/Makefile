EXEC=zdbtests
SRC=$(wildcard *.c)
OBJ=$(SRC:.c=.o)

CFLAGS=-g -std=gnu99 -O0 -W -Wall -Wextra -msse4.2 -Wno-implicit-fallthrough
LDFLAGS=-lhiredis -lpthread

CFLAGS+=$(shell pkg-config --cflags hiredis)

all: $(EXEC)

$(EXEC): $(OBJ)
	$(CC) -o $@ $^ $(LDFLAGS)

%.o: %.c
	$(CC) $(CFLAGS) -c $<

clean:
	$(RM) *.o

mrproper: clean
	$(RM) $(EXEC)
