#------------------------------------------------------------------------------
# Include source and build directories
#------------------------------------------------------------------------------
set(HDF5_VOL_RADOS_BUILD_INCLUDE_DEPENDENCIES
  ${CMAKE_CURRENT_SOURCE_DIR}
  ${CMAKE_CURRENT_BINARY_DIR}
)

#------------------------------------------------------------------------------
# Internal dependencies (exported libs)
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
# External dependencies
#------------------------------------------------------------------------------
# RADOS
option(HDF5_VOL_RADOS_USE_MOBJECT "Use mobject backend instead of RADOS." ON)
if(HDF5_VOL_RADOS_USE_MOBJECT)
  find_package(MOBJECT REQUIRED)
  if(MOBJECT_FOUND)
    set(H5VL_RADOS_USE_MOBJECT 1)
    set(HDF5_VOL_RADOS_EXT_INCLUDE_DEPENDENCIES
      ${HDF5_VOL_RADOS_EXT_INCLUDE_DEPENDENCIES}
      ${MOBJECT_INCLUDE_DIRS}
    )
    set(HDF5_VOL_RADOS_EXT_LIB_DEPENDENCIES
      ${HDF5_VOL_RADOS_EXT_LIB_DEPENDENCIES}
      ${MOBJECT_LIBRARIES}
    )
  else()
    message(FATAL_ERROR "Could not find MOBJECT, please check MOBJECT_INCLUDE_DIR.")
  endif()
else()
  find_package(RADOS REQUIRED)
  if(RADOS_FOUND)
    set(HDF5_VOL_RADOS_EXT_INCLUDE_DEPENDENCIES
      ${HDF5_VOL_RADOS_EXT_INCLUDE_DEPENDENCIES}
      ${RADOS_INCLUDE_DIRS}
    )
    set(HDF5_VOL_RADOS_EXT_LIB_DEPENDENCIES
      ${HDF5_VOL_RADOS_EXT_LIB_DEPENDENCIES}
      ${RADOS_LIBRARIES}
    )
  else()
    message(FATAL_ERROR "Could not find RADOS, please check RADOS_INCLUDE_DIR.")
  endif()
endif()

# HDF5
option(HDF5_VOL_RADOS_USE_SYSTEM_HDF5 "Use system-installed HDF5." ON)
if(HDF5_VOL_RADOS_USE_SYSTEM_HDF5)
  # We will require 1.12 once it is released
  find_package(HDF5 1.11 NO_MODULE NAMES hdf5 COMPONENTS C shared)
  if(HDF5_FOUND)
    set(HDF5_C_SHARED_LIBRARY hdf5-shared)
    if(NOT TARGET ${HDF5_C_SHARED_LIBRARY})
        message(FATAL_ERROR "Could not find hdf5 shared target, please make "
        "sure that HDF5 has ben compiled with shared libraries enabled.")
    endif()
    set(HDF5_VOL_RADOS_EXT_PKG_DEPENDENCIES
      ${HDF5_VOL_RADOS_EXT_PKG_DEPENDENCIES}
      ${HDF5_C_SHARED_LIBRARY}
    )
  else()
    # Allow for HDF5 autotools builds
    find_package(HDF5 1.11 MODULE REQUIRED)
    if(HDF5_FOUND)
      set(HDF5_VOL_RADOS_EXT_INCLUDE_DEPENDENCIES
        ${HDF5_VOL_RADOS_EXT_INCLUDE_DEPENDENCIES}
        ${HDF5_INCLUDE_DIRS}
      )
      set(HDF5_VOL_RADOS_EXT_LIB_DEPENDENCIES
        ${HDF5_VOL_RADOS_EXT_LIB_DEPENDENCIES}
        ${HDF5_LIBRARIES}
      )
    else()
      message(FATAL_ERROR "Could not find HDF5, please check HDF5_DIR.")
    endif()
  endif()
else()
  set(HDF5_EXTERNALLY_CONFIGURED 1)
  set(HDF5_INSTALL_BIN_DIR ${HDF5_VOL_RADOS_INSTALL_BIN_DIR})
  set(HDF5_INSTALL_LIB_DIR ${HDF5_VOL_RADOS_INSTALL_LIB_DIR})
  set(HDF5_INSTALL_INCLUDE_DIR ${HDF5_VOL_RADOS_INSTALL_INCLUDE_DIR})
  set(HDF5_INSTALL_DATA_DIR ${HDF5_VOL_RADOS_INSTALL_DATA_DIR})
  set(BUILD_STATIC_LIBS "OFF" CACHE BOOL "Build Static Libraries")
  set(HDF5_BUILD_CPP_LIB "OFF" CACHE BOOL "Build HDF5 C++ Library")
  set(HDF5_BUILD_EXAMPLES "OFF" CACHE BOOL "Build HDF5 Library Examples")
  set(HDF5_BUILD_HL_LIB "OFF" CACHE BOOL "Build HIGH Level HDF5 Library")
  set(HDF5_BUILD_TOOLS "OFF" CACHE BOOL "Build HDF5 Tools")
  set(HDF5_ENABLE_PARALLEL "ON" CACHE BOOL "Enable parallel build (requires MPI)")
  # TODO might need to fix HDF5_INSTALL_INCLUDE_INTERFACE from here
  set(HDF5_EXPORTED_TARGETS ${HDF5_VOL_RADOS_EXPORTED_TARGETS})
  add_subdirectory(hdf5)
  include(${HDF5_BINARY_DIR}/hdf5-config.cmake)
  set(HDF5_C_SHARED_LIBRARY hdf5-shared)
  set(HDF5_VOL_RADOS_EXPORTED_LIBS ${HDF5_C_SHARED_LIBRARY} ${HDF5_VOL_RADOS_EXPORTED_LIBS})
  get_target_property(HDF5_EXT_LIB_DEPENDENCIES ${HDF5_C_SHARED_LIBRARY} INTERFACE_LINK_LIBRARIES)
  if(HDF5_EXT_LIB_DEPENDENCIES)
    set(HDF5_VOL_RADOS_EXT_PKG_LIB_DEPENDENCIES
      ${HDF5_VOL_RADOS_EXT_PKG_LIB_DEPENDENCIES}
      ${HDF5_EXT_LIB_DEPENDENCIES}
    )
  endif()
endif()

# MPI
# Temporary workaround because HDF5 does not pull MPI dependency
find_package(MPI REQUIRED)
if(MPI_FOUND)
  set(HDF5_VOL_RADOS_EXT_INCLUDE_DEPENDENCIES
    ${HDF5_VOL_RADOS_EXT_INCLUDE_DEPENDENCIES}
    ${MPI_INCLUDE_PATH}
  )
  set(HDF5_VOL_RADOS_EXT_LIB_DEPENDENCIES
    ${HDF5_VOL_RADOS_EXT_LIB_DEPENDENCIES}
    ${MPI_LIBRARIES}
  )
else()
  message(FATAL_ERROR "Could not find MPI.")
endif()

# UUID
#find_package(UUID REQUIRED)
#if(UUID_FOUND)
#  set(HDF5_VOL_RADOS_EXT_INCLUDE_DEPENDENCIES
#    ${HDF5_VOL_RADOS_EXT_INCLUDE_DEPENDENCIES}
#    ${UUID_INCLUDE_DIRS}
#  )
#  set(HDF5_VOL_RADOS_EXT_LIB_DEPENDENCIES
#    ${HDF5_VOL_RADOS_EXT_LIB_DEPENDENCIES}
#    ${UUID_LIBRARIES}
#  )
#else()
#  message(FATAL_ERROR "Could not find libuuid, please check UUID_INCLUDE_DIR.")
#endif()

#-----------------------------------------------------------------------------
# Option to enable debug output
#-----------------------------------------------------------------------------
option(HDF5_VOL_RADOS_ENABLE_DEBUG "Turn on debug output." OFF)
if(HDF5_VOL_RADOS_ENABLE_DEBUG)
  set(DV_PLUGIN_DEBUG 1)
endif()

#-----------------------------------------------------------------------------
# Option to enable memory checker
#-----------------------------------------------------------------------------
option(HDF5_VOL_RADOS_ENABLE_MEM_TRACKING "Turn on memory checker." OFF)
if(HDF5_VOL_RADOS_ENABLE_MEM_TRACKING)
  set(DV_TRACK_MEM_USAGE 1)
endif()

#------------------------------------------------------------------------------
# Configure module header files
#------------------------------------------------------------------------------
# Set unique vars used in the autogenerated config file (symbol import/export)
configure_file(
  ${CMAKE_CURRENT_SOURCE_DIR}/H5VLrados_config.h.in
  ${CMAKE_CURRENT_BINARY_DIR}/H5VLrados_config.h
)

#------------------------------------------------------------------------------
# Set sources
#------------------------------------------------------------------------------
set(HDF5_VOL_RADOS_SRCS
  ${CMAKE_CURRENT_SOURCE_DIR}/H5VLrados.c
  ${CMAKE_CURRENT_SOURCE_DIR}/H5VLselect.c
)

#------------------------------------------------------------------------------
# Libraries
#------------------------------------------------------------------------------

# Clean up system include path first
foreach(item ${HDF5_VOL_RADOS_SYSTEM_INCLUDE_PATH})
  if(HDF5_VOL_RADOS_EXT_INCLUDE_DEPENDENCIES)
    list(REMOVE_ITEM HDF5_VOL_RADOS_EXT_INCLUDE_DEPENDENCIES ${item})
  endif()
endforeach()

# HDF5 RADOS
add_library(hdf5_vol_rados ${HDF5_VOL_RADOS_SRCS})
target_include_directories(hdf5_vol_rados
  PUBLIC  "$<BUILD_INTERFACE:${HDF5_VOL_RADOS_BUILD_INCLUDE_DEPENDENCIES}>"
          $<INSTALL_INTERFACE:${HDF5_VOL_RADOS_INSTALL_INCLUDE_INTERFACE}>
)
target_include_directories(hdf5_vol_rados
  SYSTEM PUBLIC ${HDF5_VOL_RADOS_EXT_INCLUDE_DEPENDENCIES}
)
target_link_libraries(hdf5_vol_rados
  ${HDF5_VOL_RADOS_EXPORTED_LIBS}
  ${HDF5_VOL_RADOS_EXT_LIB_DEPENDENCIES}
  ${HDF5_VOL_RADOS_EXT_PKG_DEPENDENCIES}
  )
hdf5_vol_rados_set_lib_options(hdf5_vol_rados "hdf5_vol_rados" ${HDF5_VOL_RADOS_LIBTYPE})
if(HDF5_VOL_RADOS_ENABLE_COVERAGE)
  set_coverage_flags(hdf5_vol_rados)
endif()

set(HDF5_VOL_RADOS_EXPORTED_LIBS hdf5_vol_rados ${HDF5_VOL_RADOS_EXPORTED_LIBS})

#-----------------------------------------------------------------------------
# Specify project header files to be installed
#-----------------------------------------------------------------------------
set(HDF5_VOL_RADOS_HEADERS
  ${CMAKE_CURRENT_BINARY_DIR}/H5VLrados_config.h
  ${CMAKE_CURRENT_SOURCE_DIR}/H5VLrados_public.h
)

#-----------------------------------------------------------------------------
# Add file(s) to CMake Install
#-----------------------------------------------------------------------------
install(
  FILES
    ${HDF5_VOL_RADOS_HEADERS}
  DESTINATION
    ${HDF5_VOL_RADOS_INSTALL_INCLUDE_DIR}
  COMPONENT
    headers
)

#-----------------------------------------------------------------------------
# Add Target(s) to CMake Install
#-----------------------------------------------------------------------------
install(
  TARGETS
    hdf5_vol_rados
  EXPORT
    ${HDF5_VOL_RADOS_EXPORTED_TARGETS}
  LIBRARY DESTINATION ${HDF5_VOL_RADOS_INSTALL_LIB_DIR}
  ARCHIVE DESTINATION ${HDF5_VOL_RADOS_INSTALL_LIB_DIR}
  RUNTIME DESTINATION ${HDF5_VOL_RADOS_INSTALL_BIN_DIR}
)

#-----------------------------------------------------------------------------
# Add Target(s) to CMake Install for import into other projects
#-----------------------------------------------------------------------------
install(
  EXPORT
    ${HDF5_VOL_RADOS_EXPORTED_TARGETS}
  DESTINATION
    ${HDF5_VOL_RADOS_INSTALL_DATA_DIR}/cmake/hdf5_vol_rados
  FILE
    ${HDF5_VOL_RADOS_EXPORTED_TARGETS}.cmake
)

#-----------------------------------------------------------------------------
# Export all exported targets to the build tree for use by parent project
#-----------------------------------------------------------------------------
if(NOT HDF5_VOL_RADOS_EXTERNALLY_CONFIGURED)
  export(
    TARGETS
      ${HDF5_VOL_RADOS_EXPORTED_LIBS}
    FILE
      ${HDF5_VOL_RADOS_EXPORTED_TARGETS}.cmake
  )
# TODO There is a namespace issue with the way HDF5 currently defines targets
#  if(NOT HDF5_VOL_RADOS_USE_SYSTEM_HDF5)
#    export(
#      TARGETS
#        ...
#      APPEND FILE
#        ${HDF5_VOL_RADOS_EXPORTED_TARGETS}.cmake
#      NAMESPACE hdf5::
#    )
#  endif()
endif()

#------------------------------------------------------------------------------
# Set variables for parent scope
#------------------------------------------------------------------------------

# Pkg-config configuration
if(CMAKE_BUILD_TYPE)
  string(TOLOWER ${CMAKE_BUILD_TYPE} lower_cmake_build_type)
endif()

# HDF5 RADOS package dependencies
foreach(pkg_dep ${HDF5_VOL_RADOS_EXT_PKG_DEPENDENCIES})
  set(HDF5_VOL_RADOS_PKG_DEPENDENCIES "${HDF5_VOL_RADOS_PKG_DEPENDENCIES} ${pkg_dep}")
endforeach()
set(HDF5_VOL_RADOS_PKG_DEPENDENCIES ${HDF5_VOL_RADOS_PKG_DEPENDENCIES} PARENT_SCOPE)

# HDF5 RADOS private library dependencies
foreach(exported_lib ${HDF5_VOL_RADOS_EXPORTED_LIBS})
  if(lower_cmake_build_type MATCHES "debug")
    get_target_property(HDF5_VOL_RADOS_LIBRARY ${exported_lib} DEBUG_OUTPUT_NAME)
  else()
    get_target_property(HDF5_VOL_RADOS_LIBRARY ${exported_lib} RELEASE_OUTPUT_NAME)
  endif()
  set(HDF5_VOL_RADOS_LIBRARIES "${HDF5_VOL_RADOS_LIBRARIES} -l${HDF5_VOL_RADOS_LIBRARY}")
endforeach()
set(HDF5_VOL_RADOS_LIBRARIES ${HDF5_VOL_RADOS_LIBRARIES} PARENT_SCOPE)

# HDF5 RADOS external library dependencies
# Need to generate -llib if not already passed
set(HDF5_VOL_RADOS_EXT_LIB_DEPENDENCIES
  ${HDF5_VOL_RADOS_EXT_LIB_DEPENDENCIES}
  ${HDF5_VOL_RADOS_EXT_PKG_LIB_DEPENDENCIES}
)
foreach(lib_dep ${HDF5_VOL_RADOS_EXT_LIB_DEPENDENCIES})
  # get library name
  get_filename_component(lib_name ${lib_dep} NAME_WE)
  if(lib_name MATCHES "^-l")
    # lib_name found is -lxxx
    set(HDF5_VOL_RADOS_EXT_LIB_DEPENDENCIES_LIST ${HDF5_VOL_RADOS_EXT_LIB_DEPENDENCIES_LIST} ${lib_name})
  else()
    # lib_name is /path/to/lib so get library path and name
    get_filename_component(lib_path ${lib_dep} PATH)
    string(REGEX REPLACE "^lib" "" lib_name ${lib_name})
    set(HDF5_VOL_RADOS_EXT_LIB_DEPENDENCIES_LIST ${HDF5_VOL_RADOS_EXT_LIB_DEPENDENCIES_LIST} -L${lib_path} -l${lib_name})
  endif()
endforeach()
if(HDF5_VOL_RADOS_EXT_LIB_DEPENDENCIES_LIST)
  list(REMOVE_DUPLICATES HDF5_VOL_RADOS_EXT_LIB_DEPENDENCIES_LIST)
endif()
foreach(lib_dep ${HDF5_VOL_RADOS_EXT_LIB_DEPENDENCIES_LIST})
  set(HDF5_VOL_RADOS_LIB_DEPENDENCIES "${HDF5_VOL_RADOS_LIB_DEPENDENCIES} ${lib_dep}")
endforeach()
set(HDF5_VOL_RADOS_LIB_DEPENDENCIES ${HDF5_VOL_RADOS_LIB_DEPENDENCIES} PARENT_SCOPE)

# External include dependencies
set(HDF5_VOL_RADOS_EXT_INCLUDE_DEPENDENCIES
  ${HDF5_VOL_RADOS_EXT_INCLUDE_DEPENDENCIES}
  ${HDF5_VOL_RADOS_EXT_PKG_INCLUDE_DEPENDENCIES}
)
if(HDF5_VOL_RADOS_EXT_INCLUDE_DEPENDENCIES)
  list(REMOVE_DUPLICATES HDF5_VOL_RADOS_EXT_INCLUDE_DEPENDENCIES)
endif()
foreach(inc_dep ${HDF5_VOL_RADOS_EXT_INCLUDE_DEPENDENCIES})
  set(HDF5_VOL_RADOS_INCLUDE_DEPENDENCIES "${HDF5_VOL_RADOS_INCLUDE_DEPENDENCIES} -I${inc_dep}")
endforeach()
set(HDF5_VOL_RADOS_INCLUDE_DEPENDENCIES ${HDF5_VOL_RADOS_INCLUDE_DEPENDENCIES} PARENT_SCOPE)
