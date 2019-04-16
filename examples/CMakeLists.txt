cmake_minimum_required(VERSION 2.8.12.2 FATAL_ERROR)
project(HDF5_VOL_RADOS_EXAMPLES C)

#------------------------------------------------------------------------------
# Include source and build directories
#------------------------------------------------------------------------------
include_directories(
  ${CMAKE_CURRENT_SOURCE_DIR}
  ${CMAKE_CURRENT_BINARY_DIR}
)

#------------------------------------------------------------------------------
# External dependencies
#------------------------------------------------------------------------------
# HDF5 RADOS
find_package(HDF5_VOL_RADOS REQUIRED)

#-----------------------------------------------------------------------------
# Define Sources
#-----------------------------------------------------------------------------
set(examples
  dset_open
  dset_create
  dset_wss
  dset_write
  dset_wpartial
  dset_rss
  dset_rpartial
  dset_read
  group_open
  group_create
  file_open
  file_create
)

foreach (example ${examples})
  add_executable (h5rados_${example}
    ${CMAKE_CURRENT_SOURCE_DIR}/h5rados_${example}.c
  )
  target_link_libraries(h5rados_${example} hdf5_vol_rados)
endforeach()
