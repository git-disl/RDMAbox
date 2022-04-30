SHELL = /bin/sh
INSTALL = /usr/bin/install -c
mkdir_p = mkdir -p
build_dir = /lib/modules/$(shell uname -r)
OFED_CFLAGS = -I/var/lib/dkms/mlnx-ofed-kernel/3.1/build/include -include /var/lib/dkms/mlnx-ofed-kernel/3.1/build/include/linux/compat-2.6.h
KSYMVERS = /var/lib/dkms/mlnx-ofed-kernel/3.1/build/Module.symvers
NOSTDINC_FLAGS += -I/var/lib/dkms/mlnx-ofed-kernel/3.1/build/include -include /var/lib/dkms/mlnx-ofed-kernel/3.1/build/include/linux/compat-2.6.h
ccflags-y  += -w

rpgmoduledir = /lib/modules/$(shell uname -r)/extra/rpg
rpgmodule := rpg.ko
src_dir := ./
nbdxadm := nbdxadm

all: all-spec

install: all
	$(mkdir_p) $(DESTDIR)$(rpgmoduledir)
	$(INSTALL) -m 644 $(src_dir)/$(rpgmodule) $(DESTDIR)$(rpgmoduledir)/$(rpgmodule)
	-/sbin/depmod -a
	-\cp $(nbdxadm)/nbdxadm /usr/local/bin/

uninstall:
	rm -f $(DESTDIR)$(rpgmoduledir)/$(rpgmodule)
	-/sbin/depmod -a
	-rm -f /usr/local/bin/nbdxadm

clean:
	$(MAKE) -C $(build_dir)/build SUBDIRS=`pwd` clean

obj-m := rpg.o

rpg-objs :=     \
	alf_queue.o     \
	rdmabox.o       \
	diskbox.o       \
	rpg_mempool.o   \
	radixtree.o     \
	rpg_configfs.o  \
	rpg_drv.o

all-spec:
	$(MAKE) -C $(build_dir)/build SUBDIRS=`pwd` KBUILD_EXTRA_SYMBOLS="$(KSYMVERS)" modules
