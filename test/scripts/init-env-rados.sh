#!/bin/bash

export HDF5_RADOS_ID=admin
export HDF5_RADOS_CONF=/etc/ceph/ceph.conf
export HDF5_RADOS_POOL=data
export HDF5_VOL_CONNECTOR=rados
export HDF5_PLUGIN_PATH=${HOME}/vol-rados/build/bin/
