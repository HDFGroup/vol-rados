# HDF5 RADOS VOL connector

## Table of Contents
1. [Description](#1-Description)
2. [Installation](#2-Installation)
    * [Prerequisites](#Prerequisites)
    * [Build instructions](#Build-instructions)
    * [CMake options](#CMake-options)
    * [Connector options](#Connector-options)
3. [Testing and Usage](#3-Testing-And-Usage)
4. [More information](#4-More-Information)

## 1. Description

The HDF5 RADOS VOL connector is a Virtual Object Layer (VOL) connector for HDF5
that allows HDF5 applications to interface with object-, block- and file-based
storage by using the [RADOS API](https://docs.ceph.com/en/quincy/rados/api/librados-intro/).
It supports access to [Ceph-based](https://docs.ceph.com/en/quincy/start/intro/)
storage via `librados` and also supports access to [Mobject-based](https://github.com/mochi-hpc/mobject)
storage.

The VOL connector is built as a plugin library that is external to HDF5 and can
be loaded in an HDF5 application at runtime.

## 2. Installation

### Prerequisites

To build the RADOS VOL connector, the following are required:

+ `libhdf5` - The [HDF5](https://www.hdfgroup.org/downloads/hdf5/) library.
            Minimum version required is 1.13.3, compiled with
            support for parallel I/O.

+ An MPI library implementation

Additional dependencies are required, depending on which type of storage is of interest.
If interested in accessing Mobject-based storage (the default mode of operation), the
following are required:

+ [Mobject](https://github.com/mochi-hpc/mobject)

If interested in accessing Ceph-based storage, the following are required:

+ [Ceph](https://docs.ceph.com/en/quincy/start/intro/)

+ `librados` - The [RADOS API library](https://docs.ceph.com/en/quincy/rados/api/librados-intro/).

Compiled libraries must either exist in the system's library paths or must be
pointed to during the RADOS VOL connector build process. The simplest way to
ensure these libraries can be located is to install and use them via [spack](https://spack.io/).

For example, Mobject can be installed via spack according to the instructions listed in the
project's README. Once installed, a simple

```bash
$ spack load mobject
```

should be sufficient for ensuring the Mobject libraries are available for
the VOL connector build process.

### Build instructions

The HDF5 RADOS VOL connector is built using CMake. CMake version 2.8.12.2 or
greater is required for building the connector itself, but version 3.1 or
greater is required to build the connector's tests.

After obtaining the connector's source code, you should first create a build
directory within the source tree:

```bash
$ cd vol-rados
$ mkdir build
$ cd build
```

Then, if all of the required components are available, the connector can be
built by using `cmake` (or `ccmake`):

```bash
$ cmake [cmake options] [connector options] ..
```

```bash
$ ccmake ..
```

If using `cmake`, then CMake options and options for the RADOS VOL connector should
be passed on the command line with `-D`. If using `ccmake`, suitable options can be
chosen in the interface. Some of these options may be needed if, for example, the
required components mentioned previously are not located in default paths.

If using `ccmake`, setting include directory and library paths may require you to
toggle to the advanced mode by typing `'t'`. Once all options have been chosen in
the `ccmake` interface, you should type `'c'` (this may need to be done multiple times
as `ccmake` adjusts to settings chosen). Once you are done and do not see any errors,
type `'g'` to generate build files for the connector.

Once the project is configured via CMake, simply do:

```bash
$ make && make install
```

to build and install the connector plugin library. Note that this command may differ
depending on the platform and CMake generator used.

### CMake options

The following are some CMake options that may be desirable to set when building
the RADOS VOL connector:

  * `CMAKE_INSTALL_PREFIX` - This option controls the install directory that the resulting output files are written to. The default value is `/usr/local`.
  * `CMAKE_BUILD_TYPE` - This option controls the type of build used for the VOL connector. Valid values are Release, Debug, RelWithDebInfo and MinSizeRel; the default build type is RelWithDebInfo.

### Connector options

The following connector-specific CMake options are available when building the
RADOS VOL connector:

  * `HDF5_VOL_RADOS_USE_MOBJECT` - This option specifies whether the VOL connector
  should be built with Mobject-based storage support. This is the default mode
  for the connector. If this option is disabled, the connector will instead be
  built with support for Ceph-based storage via `librados`.
  * `BUILD_TESTING` - This option is used to enable/disable building of the
  RADOS VOL connector's tests. The default value is `OFF`.
  * `BUILD_EXAMPLES` - This option is used to enable/disable building of the
  RADOS VOL connector's HDF5 examples. The default value is `OFF`.

## 3. Testing and Usage

The RADOS VOL connector contains no tests itself, but rather leverages the HDF5
[VOL tests](https://github.com/HDFGroup/vol-tests) project for testing. Building
of these tests is disabled by default, but can be enabled by setting the
`BUILD_TESTING` option to `ON`.

For information on how to use the RADOS VOL connector with an HDF5 application,
including setting up and running a Mobject or Ceph server, please refer to the
RADOS VOL User's Guide under _docs/users_guide.pdf_.

## 4. More Information

+ Mochi
    + https://mochi.readthedocs.io/en/latest/

+ Mobject
    + https://github.com/mochi-hpc/mobject

+ Ceph
    + https://docs.ceph.com/en/quincy/start/intro/

+ HDF5
    + https://www.hdfgroup.org/solutions/hdf5/
    + https://portal.hdfgroup.org/display/HDF5/Virtual+Object+Layer
