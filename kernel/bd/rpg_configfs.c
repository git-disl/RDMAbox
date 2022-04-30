/*
 * RDMAbox 
 * Copyright (c) 2021 Georgia Institute of Technology
 *
 *  
 * Copyright (c) 2013 Mellanox Technologies��. All rights reserved.
 *
 * This software is available to you under a choice of one of two licenses.
 * You may choose to be licensed under the terms of the GNU General Public
 * License (GPL) Version 2, available from the file COPYING in the main
 * directory of this source tree, or the Mellanox Technologies�� BSD license
 * below:
 *
 *      - Redistribution and use in source and binary forms, with or without
 *        modification, are permitted provided that the following conditions
 *        are met:
 *
 *      - Redistributions of source code must retain the above copyright
 *        notice, this list of conditions and the following disclaimer.
 *
 *      - Redistributions in binary form must reproduce the above
 *        copyright notice, this list of conditions and the following
 *        disclaimer in the documentation and/or other materials
 *        provided with the distribution.
 *
 *      - Neither the name of the Mellanox Technologies�� nor the names of its
 *        contributors may be used to endorse or promote products derived from
 *        this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#include "debug.h"

#include "rpg_drv.h"
#include "rdmabox.h"

int created_portals = 0;

#define cgroup_to_RDMABOX_session(x) container_of(x, struct RDMABOX_session, session_cg)
#define cgroup_to_RDMABOX_device(x) container_of(x, struct RDMABOX_file, dev_cg)

// bind in RDMABOX_device_item_ops
static ssize_t device_attr_store(struct config_item *item,
    struct configfs_attribute *attr,
    const char *page, size_t count)
{
  struct RDMABOX_session *RDMABOX_session;
  struct RDMABOX_file *RDMABOX_device;
  char xdev_name[MAX_RDMABOX_DEV_NAME];
  ssize_t ret;

  printk("rpg %s\n", __func__);
  RDMABOX_session = cgroup_to_RDMABOX_session(to_config_group(item->ci_parent));
  RDMABOX_device = cgroup_to_RDMABOX_device(to_config_group(item));

  sscanf(page, "%s", xdev_name);
  if(RDMABOX_file_find(RDMABOX_session, xdev_name)) {
    printk("rpg %s:Device already exists: %s",__func__, xdev_name);
    return -EEXIST;
  }
  // device is RDMABOX_file
  ret = RDMABOX_create_device(RDMABOX_session, xdev_name, RDMABOX_device); 
  if (ret) {
    printk("rpg %s:failed to create device %s\n",__func__, xdev_name);
    return ret;
  }

  return count;
}

// bind in RDMABOX_device_item_ops
static ssize_t state_attr_show(struct config_item *item,
    struct configfs_attribute *attr,
    char *page)
{
  struct RDMABOX_file *RDMABOX_device;
  ssize_t ret;

  printk("rpg %s\n", __func__);
  RDMABOX_device = cgroup_to_RDMABOX_device(to_config_group(item));

  ret = snprintf(page, PAGE_SIZE, "%s\n", RDMABOX_device_state_str(RDMABOX_device));

  return ret;
}

// bind in RDMABOX_device_type
#if LINUX_VERSION_CODE >= KERNEL_VERSION(4, 4, 0)
#else
static struct configfs_item_operations RDMABOX_device_item_ops = {
  .store_attribute = device_attr_store,
  .show_attribute = state_attr_show,
};
#endif

// bind in RDMABOX_device_item_attrs
static struct configfs_attribute device_item_attr = {
  .ca_owner       = THIS_MODULE,
  .ca_name        = "device",
  .ca_mode        = S_IWUGO,
#if LINUX_VERSION_CODE >= KERNEL_VERSION(4, 4, 0)
  .show		  = state_attr_show,
  .store	  = device_attr_store,
#endif

};
// bind in RDMABOX_device_item_attrs
static struct configfs_attribute state_item_attr = {
  .ca_owner       = THIS_MODULE,
  .ca_name        = "state",
  .ca_mode        = S_IRUGO,

};

// bind in RDMABOX_device_type
static struct configfs_attribute *RDMABOX_device_item_attrs[] = {
  &device_item_attr,
  &state_item_attr,
  NULL,
};

// defined in RDMABOX_device_make_group
static struct config_item_type RDMABOX_device_type = {
#if LINUX_VERSION_CODE >= KERNEL_VERSION(4, 4, 0)
#else
  .ct_item_ops    = &RDMABOX_device_item_ops,
#endif
  .ct_attrs       = RDMABOX_device_item_attrs,
  .ct_owner       = THIS_MODULE,
};

// bind in RDMABOX_session_devices_group_ops
static struct config_group *RDMABOX_device_make_group(struct config_group *group,
    const char *name)
{
  struct RDMABOX_session *RDMABOX_session;
  struct RDMABOX_file *RDMABOX_file;

  printk("rpg %s: name=%s\n", __func__, name);
  RDMABOX_file = kzalloc(sizeof(*RDMABOX_file), GFP_KERNEL); // allocate space for device
  if (!RDMABOX_file) {
    printk("rpg %s:RDMABOX_file alloc failed\n",__func__);
    return NULL;
  }

  spin_lock_init(&RDMABOX_file->state_lock);
  if (RDMABOX_set_device_state(RDMABOX_file, DEVICE_OPENNING)) {  // change state
    printk("rpg %s:device %s: Illegal state transition %s -> openning\n",__func__,
	RDMABOX_file->dev_name,
	RDMABOX_device_state_str(RDMABOX_file));
    goto err;
  }

  sscanf(name, "%s", RDMABOX_file->dev_name);
  RDMABOX_session = cgroup_to_RDMABOX_session(group);
  spin_lock(&RDMABOX_session->devs_lock);
  list_add(&RDMABOX_file->list, &RDMABOX_session->devs_list);
  spin_unlock(&RDMABOX_session->devs_lock);

  config_group_init_type_name(&RDMABOX_file->dev_cg, name, &RDMABOX_device_type);// define & bind RDMABOX_device_type

  return &RDMABOX_file->dev_cg;
err:
  kfree(RDMABOX_file);
  return NULL;
}

// bind in RDMABOX_session_devices_group_ops
static void RDMABOX_device_drop(struct config_group *group, struct config_item *item)
{
  struct RDMABOX_file *RDMABOX_device;
  struct RDMABOX_session *RDMABOX_session;

  printk("rpg %s\n", __func__);
  RDMABOX_session = cgroup_to_RDMABOX_session(group);
  RDMABOX_device = cgroup_to_RDMABOX_device(to_config_group(item));
  RDMABOX_destroy_device(RDMABOX_session, RDMABOX_device);
  kfree(RDMABOX_device);
}

// bind in RDMABOX_session_item_ops
static ssize_t portal_attr_store(struct config_item *citem,
#if LINUX_VERSION_CODE >= KERNEL_VERSION(4, 4, 0)
#else
    struct configfs_attribute *attr,
#endif
    const char *buf,size_t count)
{
  char rdma[MAX_PORTAL_NAME] = "rdma://" ;
  struct RDMABOX_session *RDMABOX_session;

  printk("rpg %s: buf=%s\n", __func__, buf);
  sscanf(strcat(rdma, buf), "%s", rdma);
  if(RDMABOX_session_find_by_portal(&g_RDMABOX_sessions, rdma)) {
    printk("rpg %s:Portal already exists: %s",__func__, buf);
    return -EEXIST;
  }

  RDMABOX_session = cgroup_to_RDMABOX_session(to_config_group(citem));
  // session is created here
  if (RDMABOX_session_create(rdma, RDMABOX_session)) {
    printk("rpg %s : Couldn't create new session with %s\n",__func__, rdma);
    return -EINVAL;
  }

  return count;
}

// bind in RDMABOX_session_type
static struct configfs_group_operations RDMABOX_session_devices_group_ops = {
  .make_group     = RDMABOX_device_make_group,
  .drop_item      = RDMABOX_device_drop,
};

// bind in RDMABOX_session_type
#if LINUX_VERSION_CODE >= KERNEL_VERSION(4, 4, 0)
#else
static struct configfs_item_operations RDMABOX_session_item_ops = {
  .store_attribute = portal_attr_store,
};
#endif

// bind in RDMABOX_session_item_attrs
static struct configfs_attribute portal_item_attr = {
  .ca_owner       = THIS_MODULE,
  .ca_name        = "portal",
  .ca_mode        = S_IWUGO,
#if LINUX_VERSION_CODE >= KERNEL_VERSION(4, 4, 0)
  .store  	  = portal_attr_store,
#endif	
};
// bind in RDMABOX_session_type
static struct configfs_attribute *RDMABOX_session_item_attrs[] = {
  &portal_item_attr,
  NULL,
};

// bind in RDMABOX_session_make_group()
static struct config_item_type RDMABOX_session_type = {
#if LINUX_VERSION_CODE >= KERNEL_VERSION(4, 4, 0)
#else
  .ct_item_ops    = &RDMABOX_session_item_ops,
#endif
  .ct_attrs       = RDMABOX_session_item_attrs,
  .ct_group_ops   = &RDMABOX_session_devices_group_ops,
  .ct_owner       = THIS_MODULE,
};

// defined in RDMABOX_group_ops
static struct config_group *RDMABOX_session_make_group(struct config_group *group,
    const char *name)
{
  struct RDMABOX_session *RDMABOX_session;

  printk("rpg %s: name=%s\n", __func__, name);
  RDMABOX_session = kzalloc(sizeof(*RDMABOX_session), GFP_KERNEL); // allocate the space for RDMABOX_session
  if (!RDMABOX_session) {
    printk("rpg %s:failed to allocate IS session\n",__func__);
    return NULL;
  }

  INIT_LIST_HEAD(&RDMABOX_session->devs_list);
  spin_lock_init(&RDMABOX_session->devs_lock);
  mutex_lock(&g_lock);
  list_add(&RDMABOX_session->list, &g_RDMABOX_sessions);
  created_portals++;
  mutex_unlock(&g_lock);

  config_group_init_type_name(&(RDMABOX_session->session_cg), name, &RDMABOX_session_type); // bind session with RDMABOX_session_type (item_type)

  return &(RDMABOX_session->session_cg);

}

// defined in RDMABOX_group_ops
static void RDMABOX_session_drop(struct config_group *group, struct config_item *item)
{
  struct RDMABOX_session *RDMABOX_session;

  printk("rpg %s\n", __func__);
  RDMABOX_session = cgroup_to_RDMABOX_session(to_config_group(item));
  RDMABOX_session_destroy(RDMABOX_session);  // call RDMABOX_session_destroy
  kfree(RDMABOX_session);
}

static struct configfs_group_operations RDMABOX_group_ops = {
  .make_group     = RDMABOX_session_make_group,
  .drop_item      = RDMABOX_session_drop,
};

static struct config_item_type RDMABOX_item = {
  .ct_group_ops   = &RDMABOX_group_ops,
  .ct_owner       = THIS_MODULE,
};

static struct configfs_subsystem RDMABOX_subsys = {
  .su_group = {
    .cg_item = {
      .ci_namebuf = "rpg",
      .ci_type = &RDMABOX_item,
    },
  },

};

int RDMABOX_create_configfs_files(void)
{
  int err = 0;

  printk("rpg %s\n", __func__);
  config_group_init(&RDMABOX_subsys.su_group);
  mutex_init(&RDMABOX_subsys.su_mutex);

  err = configfs_register_subsystem(&RDMABOX_subsys);
  if (err) {
    printk("rpg %s:Error %d while registering subsystem %s\n",__func__,
	err, RDMABOX_subsys.su_group.cg_item.ci_namebuf);
  }

  return err;
}

void RDMABOX_destroy_configfs_files(void)
{
  configfs_unregister_subsystem(&RDMABOX_subsys);
}
