cd ../urdmabox/
make clean
make

cd ../ramdisk/ramdisk/
rm -rf ramdisk
make clean
make
make ramdisk

cd ../../daemon
make clean
make

cd ~/RDMAbox/userspace/setup

