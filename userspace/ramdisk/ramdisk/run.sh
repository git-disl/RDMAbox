if [ "$1" ]; then
  sudo modprobe ib_ipoib
  sudo ifconfig ib0 $1/24
  sudo umount -f ramdisk
  sudo umount -f ramdisk
  rm -rf /users/jbae91/ramdisktest
  mkdir -p /users/jbae91/ramdisktest
  mount
  #./ramdisk /users/jbae91/ramdisktest 10000
  #./ramdisk /jbaedata/mongodb -o allow_root -o allow_other
  ./ramdisk /users/jbae91/ramdisktest -o allow_root -o allow_other -s # fuse single thread
  #./ramdisk /users/jbae91/ramdisktest -o allow_root -o allow_other # fuse multi thread
else
  echo "run.sh <IPaddr>"
fi
