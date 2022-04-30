sudo insmod ~/RDMAbox/kernel/bd/rpg.ko
sudo mount -t configfs none /sys/kernel/config

sudo ~/RDMAbox/kernel/bd/nbdxadm/nbdxadm -o create_host -i $1 -p ~/RDMAbox/kernel/setup/bd_portal.list
sudo ~/RDMAbox/kernel/bd/nbdxadm/nbdxadm -o create_device -i $1 -d $1

ls /dev/rpg$1
sudo mkswap /dev/rpg$1
sudo swapon /dev/rpg$1

