CFLAGS  := -w -g -std=gnu99
LD      := gcc
LDLIBS  := ${LDLIBS} -lrdmacm -libverbs -lpthread -lrt -ldl -lm
SRC = urdmabox.c bitmap.c lfqueue.c radixtree.c rax.c rpg_mempool.c
OBJ = $(SRC:.c=.o)
OUT = liburdmabox.a
APPS    := sobj

$(OUT): $(OBJ)
	ar rcs $(OUT) $(OBJ)

all: ${APPS}

urdmabox: urdmabox.o bitmap.o lfqueue.o
	        ${LD} ${CFLAGS} -o $@ $^ ${LDLIBS}

sobj:
	        ${LD} ${CFLAGS} -fPIC -shared -o liburdmabox.so ${SRC} ${LDLIBS}

clean:
	        rm -f *.o *.a *.so ${APPS}


