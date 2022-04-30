.PHONY: clean

#CFLAGS  := -Wall -Werror -g
CFLAGS  := -Werror -g
LD      := gcc
LDLIBS  := ${LDLIBS} -lrdmacm -libverbs -lpthread -lrt

APPS    := daemon

all: ${APPS}

#daemon: rdma-common.o daemon.o migration.o
daemon: rping.o
	${LD} -o $@ $^ ${LDLIBS}

clean:
	rm -f *.o ${APPS}

