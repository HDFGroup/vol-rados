# - Try to find MOBJECT
# Once done this will define
#  MOBJECT_FOUND - System has MOBJECT
#  MOBJECT_INCLUDE_DIRS - The MOBJECT include directories
#  MOBJECT_LIBRARIES - The libraries needed to use MOBJECT

find_package(PkgConfig)
pkg_check_modules(PC_MOBJECT mobject-store)

find_path(MOBJECT_INCLUDE_DIR librados-mobject-store.h
  HINTS ${PC_MOBJECT_INCLUDEDIR} ${PC_MOBJECT_INCLUDE_DIRS}
  PATHS /usr/local/include /usr/include)

find_path(MARGO_INCLUDE_DIR margo.h
  HINTS ${PC_MOBJECT_INCLUDEDIR} ${PC_MOBJECT_INCLUDE_DIRS}
  PATHS /usr/local/include /usr/include)

find_path(MERCURY_INCLUDE_DIR mercury.h
  HINTS ${PC_MOBJECT_INCLUDEDIR} ${PC_MOBJECT_INCLUDE_DIRS}
  PATHS /usr/local/include /usr/include)

find_path(ABT_INCLUDE_DIR abt.h
  HINTS ${PC_MOBJECT_INCLUDEDIR} ${PC_MOBJECT_INCLUDE_DIRS}
  PATHS /usr/local/include /usr/include)

find_library(MOBJECT_LIBRARY NAMES mobject-store
  HINTS ${PC_MOBJECT_LIBDIR} ${PC_MOBJECT_LIBRARY_DIRS}
  PATHS /usr/local/lib64 /usr/local/lib /usr/lib64 /usr/lib)

set(MOBJECT_INCLUDE_DIRS
  ${MOBJECT_INCLUDE_DIR}
  ${MARGO_INCLUDE_DIR}
  ${MERCURY_INCLUDE_DIR}
  ${ABT_INCLUDE_DIR}
  )
set(MOBJECT_LIBRARIES ${MOBJECT_LIBRARY})

include(FindPackageHandleStandardArgs)
# handle the QUIETLY and REQUIRED arguments and set MOBJECT_FOUND to TRUE
# if all listed variables are TRUE
find_package_handle_standard_args(MOBJECT DEFAULT_MSG
                                  MOBJECT_INCLUDE_DIR MARGO_INCLUDE_DIR
                                  MERCURY_INCLUDE_DIR ABT_INCLUDE_DIR
                                  MOBJECT_LIBRARY)

mark_as_advanced(MOBJECT_INCLUDE_DIR MOBJECT_LIBRARY)

