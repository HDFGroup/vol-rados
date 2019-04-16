#!/bin/bash

echo "Running build script from repository"
echo "(current dir is: $PWD)"

module load GCC/7.2.0-2.29
source <(spack module tcl loads --dependencies rados)
source <(spack module tcl loads --dependencies hdf5@rados-develop)

# store the current directory in a local variable to get back to it later
export HDF5_VOL_RADOS_ROOT=/mnt/hdf/jsoumagne/rados_vol_test

# set up testing configuration
export HDF5_VOL_RADOS_BUILD_CONFIGURATION="Debug"
export HDF5_VOL_RADOS_DASHBOARD_MODEL="Nightly"
export HDF5_VOL_RADOS_DO_COVERAGE="true"
export HDF5_VOL_RADOS_DO_MEMCHECK="false"
#export HDF5_VOL_RADOS_MEMORYCHECK_TYPE="AddressSanitizer"

export CC=`which gcc`
export GCOV=`which gcov`

# get back to the testing script location
pushd $HDF5_VOL_RADOS_ROOT
ctest -S $HDF5_VOL_RADOS_ROOT/source/test/scripts/jelly_script.cmake -VV --output-on-failure 2>&1 > $HDF5_VOL_RADOS_ROOT/last_build.log
popd

