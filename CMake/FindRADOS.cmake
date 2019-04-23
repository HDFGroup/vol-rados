# - Try to find RADOS
# Once done this will define
#  RADOS_FOUND - System has RADOS
#  RADOS_INCLUDE_DIRS - The RADOS include directories
#  RADOS_LIBRARIES - The libraries needed to use RADOS

find_package(PkgConfig)
pkg_check_modules(PC_RADOS rados)

find_path(RADOS_INCLUDE_DIR rados/librados.h
  HINTS ${PC_RADOS_INCLUDEDIR} ${PC_RADOS_INCLUDE_DIRS}
  PATHS /usr/local/include /usr/include)

find_library(RADOS_LIBRARY NAMES rados
  HINTS ${PC_RADOS_LIBDIR} ${PC_RADOS_LIBRARY_DIRS}
  PATHS /usr/local/lib64 /usr/local/lib /usr/lib64 /usr/lib)

set(RADOS_INCLUDE_DIRS ${RADOS_INCLUDE_DIR})
set(RADOS_LIBRARIES ${RADOS_LIBRARY})

include(FindPackageHandleStandardArgs)
# handle the QUIETLY and REQUIRED arguments and set RADOS_FOUND to TRUE
# if all listed variables are TRUE
find_package_handle_standard_args(RADOS DEFAULT_MSG
                                  RADOS_INCLUDE_DIR RADOS_LIBRARY)

mark_as_advanced(RADOS_INCLUDE_DIR RADOS_LIBRARY)

