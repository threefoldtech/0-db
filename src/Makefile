EXEC=zdb
SRC=$(wildcard *.c)
OBJ=$(SRC:.c=.o)

CFLAGS=-g -W -Wall -O2 -msse4.2 -Wno-implicit-fallthrough
LDFLAGS=-rdynamic 

all: $(EXEC)

release: CFLAGS+=-DRELEASE
release: $(EXEC)

$(EXEC): $(OBJ)
	$(CC) -o $@ $^ $(LDFLAGS)

%.o: %.c
	$(CC) $(CFLAGS) -c $<

clean:
	$(RM) *.o

mrproper: clean
	$(RM) $(EXEC)
