LIB = libzdb
SRC = $(wildcard *.c)
OBJ = $(SRC:.c=.o)

CFLAGS += -g -fPIC -std=gnu11 -O0 -W -Wall -Wextra -msse4.2 -Wno-implicit-fallthrough
LDFLAGS += -rdynamic

# grab version from git, if possible
REVISION := $(shell git describe --abbrev=8 --dirty --always --tags)
ifneq ($(REVISION),)
	CFLAGS += -DZDB_REVISION=\"$(REVISION)\"
endif

ifeq ($(STATIC),1)
	LDFLAGS += -static
endif

ifeq ($(COVERAGE),1)
	CFLAGS += -coverage -fprofile-arcs -ftest-coverage
	LDFLAGS += -lgcov --coverage
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
