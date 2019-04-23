#------------------------------------------------------------------------------
# Mobject client config
#------------------------------------------------------------------------------

set(HDF5_VOL_TEST_PLUGIN_PATH ${CMAKE_RUNTIME_OUTPUT_DIRECTORY})
set(HDF5_VOL_TEST_PLUGIN_NAME "rados")

set(MOBJECT_CLIENT_ENV_VARS
  "HDF5_PLUGIN_PATH=${HDF5_VOL_TEST_PLUGIN_PATH}"
  "HDF5_VOL_CONNECTOR=${HDF5_VOL_TEST_PLUGIN_NAME}"
)

#------------------------------------------------------------------------------
# Test config
#------------------------------------------------------------------------------

# For compatibility reasons, we also define MPIEXEC
#set(MPIEXEC_EXECUTABLE "${ORTERUN_EXECUTABLE}" CACHE FILEPATH "Executable for running MPI programs." FORCE)
#set(MPIEXEC_SERVER_PREFLAGS "${DAOS_SERVER_PREFLAGS}")
#set(MPIEXEC_SERVER_POSTFLAGS "${DAOS_SERVER_POSTFLAGS}")
#set(MPIEXEC_PREFLAGS "${DAOS_CLIENT_PREFLAGS}")
#set(MPIEXEC_MAX_NUMPROCS "1" CACHE STRING
#  "Maximum number of processors available to run MPI applications."
#  FORCE
#)

#set(HDF5_VOL_TEST_SERVER ${DAOS_SERVER_EXECUTABLE})
# Do not forget to include env command
#set(H5VL_TEST_ENV_VARS "env ${DAOS_CLIENT_ENV_VARS}")

# Test will start running after following line is printed
#set(H5VL_TEST_SERVER_START_MSG "DAOS I/O server")
#set(H5VL_TEST_INIT_COMMAND "killall -9 daos_io_server")

#------------------------------------------------------------------------------
# VOL tests
#------------------------------------------------------------------------------
set(HDF5_VOL_TEST_EXTERNALLY_CONFIGURED TRUE)
set(HDF5_VOL_TEST_ENABLE_PARALLEL "ON" CACHE BOOL "Enable testing in parallel (requires MPI).")
if(NOT EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/vol/CMakeLists.txt)
  message(STATUS "VOL test submodule not found, checking it out")
  # get newest submodule info
  execute_process(
    COMMAND "${GIT_EXECUTABLE}" submodule update --init --recursive
    WORKING_DIRECTORY "${PROJECT_SOURCE_DIR}"
    OUTPUT_VARIABLE GIT_SUBMODULE_UPDATE_OUTPUT
  )
  message(STATUS ${GIT_SUBMODULE_UPDATE_OUTPUT})
endif()
add_subdirectory(vol)
