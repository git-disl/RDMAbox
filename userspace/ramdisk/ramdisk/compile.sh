rm -rf ramdisk
make clean
make
make ramdisk
rm -rf test
gcc -w test.c -otest -lpthread
