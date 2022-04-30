CC = gcc
CFLAGS = -w -g -pthread -fPIC -D_FILE_OFFSET_BITS=64 -std=gnu99
LDLIBS := ${LDLIBS} -lrdmacm -libverbs -lpthread -lrt -ldl -lm
#SRC = ../../urdmabox/urdmabox.c ../../urdmabox/bitmap.c ../../urdmabox/lfqueue.c ../../urdmabox/rpg_mempool.c ../../urdmabox/radixtree.c ../../urdmabox/rax.c
RAMDISK_SRC = filesystem.c
#OBJ = $(SRC:.c=.o)
#OUT = ../../urdmabox/liburdmabox.a

#$(OUT): $(OBJ)
#	ar rcs $(OUT) $(OBJ)

ramdisk:
	$(CC) -L../../urdmabox $(CFLAGS) ${RAMDISK_SRC} `pkg-config fuse3 --cflags --libs` -o ramdisk -lrt -lurdmabox ${LDLIBS} #fuse3

test:
	$(CC) $(CFLAGS) -otest test.c

clean:  
	rm -f ramdisk
	rm -rf test
