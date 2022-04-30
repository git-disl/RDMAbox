cd ../ramdisk/ramdisk/

if [ "$1" ]; then
  sudo modprobe ib_ipoib
  sudo ifconfig ib0 $1/24
  sudo umount -f ramdisk
  sudo umount -f ramdisk
  rm -rf ~/ramdisktest
  mkdir -p ~/ramdisktest
  mount
  ./ramdisk ~/ramdisktest -o allow_root -o allow_other -s
else
  echo "run.sh <IPaddr>"
fi
