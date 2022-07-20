# RADOS VOL

## Dependencies
- (parallel) HDF5
- MPI
and
either Mobject or Rados
if Mobject:
- Mobject
- Margo
- Mercury
- ABT

if Rados:
- Ceph
- librados

## Build Commands
For all of these, you want to run make after, for HDF5 you may want to make install also.

### Build with Rados instead of MObject (MObject is default)
`mkdir build; cd build`
`cmake .. -DHDF5_VOL_RADOS_USE_MOBJECT=OFF`

### Building HDF5
`cmake -DHDF5_ENABLE_PARALLEL=ON ..`

### Building vol tests
`cmake -DCMAKE_BUILD_TYPE:STRING=Release -DHDF5_VOL_TEST_ENABLE_PARALLEL=ON ..`

It's worth pointing out that HDF5 itself does not do any initialization of MPI, which means this is entirely up to the client program. The rados vol connector does require MPI to be initialized somewhere in order to function. Since our client program in this case is the vol tests, we need to make sure we enforce the vol tests using parallel HDF5, and the enable parallel is how we do that.

## Environment Variables
There are some environment variables which must be set in order to use the Ceph rados vol connector, related to connecting to rados

HDF5_RADOS_ID = admin
HDF5_RADOS_CONF = /etc/ceph/ceph.conf
HDF5_RADOS_POOL = data
MOBJECT_CLUSTER_FILE=/home/kbateman/mobject/mobject.ssg

Also see https://docs.ceph.com/en/latest/rados/api/librados-intro/ for more info about librados

For testing
HDF5_VOL_CONNECTOR=rados
HDF5_PLUGIN_PATH=/home/kbateman/vol-rados/build/bin/

## Setting up Ceph

You need to set up Ceph in order to use the rados vol. Here are some tips based on what worked for me

You need docker or podman installed, I used docker

You need ntp or chrony installed, I used ntp

You need to be running an ssh server on all cluster nodes, I used openssh. You're also supposed to configure ssh across the cluster, see the preflight checklist https://docs.ceph.com/en/octopus/rados/deployment/preflight-checklist/ . I just used my existing ssh key and copied that up to the server once I had a user.

You need python 3, LVM2, and systemd. I'm pretty sure you won't have to worry about these. Python 3 is fairly standard now, LVM is installed automatically with cephadm in my experience, and systemd is usually included with your OS.

I used cephadm, it can be installed as a package on Ubuntu. The relevant command for bootstrapping the Ceph installation is:

`cephadm bootstrap --mon-ip <ip> --single-host-defaults`

If you're replacing an existing configuration, you may want to add `--allow-overwrite`

The flag `--single-host-defaults` sets up Ceph for a single host, turns out there are some replication settings that cause problems if you don't have it. Older versions of Ceph don't have this command, so you may need to change the configuration manually later. According to Ceph documentation, the configuration options set are as follows:

`global/osd_crush_chooseleaf_type = 0
global/osd_pool_default_size = 2
mgr/mgr_standby_modules = False`

The mon-ip is going to take some explanation. Ceph typically expects a distributed system with a manager node, monitor node, and OSD nodes. You can put them all on the same node, but this is not expected. For my simple deployment, though, I did that anyway. At bootstrap time, you're deploying the manager on the node you're running it on and the monitor on the IP specified. I don't believe OSDs are deployed by default, but they need to be added afterward. The IP specified can be the same as the node you're running the command on, but it needs to exist and be running an ssh server. Since I was using a virtual machine, my IPs were always in the range "10.0.0.*", but you can determine them using a command like "ip a".

I used virtualization, because Ceph cluster deployment has quirks and I found being able to conveniently manipulate the hardware and software available was the best for my usecase. This is especially helpful because OSDs in Ceph expect completely formatted drives. You can deploy them on a partition, but this is more involved and I didn't do it myself. Better to just create a virtual device in a virtual machine.

One issue I ran into is that Ceph can be picky about your Linux installation. When I attempted to use Ubuntu 22.04, I found it failing and while I didn't fully diagnose the issue I believe it was to do with https://download.ceph.com/ not having a supported release for the latest Ubuntu at the time. Under https://download.ceph.com/debian-quincy/dists/ at the time of my testing, there was no subdirectory for "Jammy" which is the name of the 22.04 distro. I expect this to be fixed eventually, but I recommend checking to make sure your version of your operating system is supported by Ceph. To fix this in my case, I simply switched to 20.04 (not a problem since I was using virtualization)

After bootstrapping, you need at least one OSD. I just created a virtual hard disk for this purpose (again, using virtualization). It's best to check that Ceph can recognize the hard disk by using the "ceph orch device ls" command. If it is visible, then you can try "ceph orch apply osd --all-available-devices", adding the flag "--dry-run" if you want to test without actually putting the OSDs out first.

All commands from Ceph have to be run as root. That includes "ceph" and "cephadm", among others. You can install both of these commands from the Ubuntu repositories. The cephadm command is sufficient for managing the cluster, but you may need to access a special Ceph shell with the login credentials specified by cephadm upon bootstrapping if you want to use "ceph" commands, which are needed to manage the cluster. This is how I did it, so I can't speak to installing the Ceph command separately (I think it should work, but I didn't confirm it).

Also, note that "ceph status" will tell you the status of the cluster. It's generally useful. Unlike compiler warnings, "HEALTH_WARN" in this case means something is definitely wrong.

If you mess something up, you might need or want to unlink the old Ceph stuff so you aren't running broken Ceph monitors or something. Most Ceph services are in "/etc/systemd/system/multi-user.target.wants/" or "/etc/systemd/system/" in Ubuntu, all with "ceph" in their name. If you want to destroy a Ceph installation, you should do "cephadm rm-cluster --fsid <fsid>", and follow it up by calling "lvremove" on the logical volumes you created for OSDs. Another command I found useful in this regard was "ceph-volume lvm zap </dev/whatever>", which clears the data on a drive (you need this and lvremove if you want to make a drive that was previously used for an OSD usable again).

We also have to create a pool so that we can use our Ceph installation with Rados.
`ceph osd pool create data 1 1 replicated`
