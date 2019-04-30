/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 * Copyright by The HDF Group.                                               *
 * All rights reserved.                                                      *
 *                                                                           *
 * This file is part of the HDF5 RADOS VOL connector. The full copyright     *
 * notice, including terms governing use, modification, and redistribution,  *
 * is contained in the COPYING file, which can be found at the root of the   *
 * source code distribution tree.                                            *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

/*
 * Purpose:	The public header file for the RADOS VOL plugin.
 */

#ifndef H5VLrados_public_H
#define H5VLrados_public_H

#include "H5VLrados_config.h"

#include <mpi.h>

/* Public headers needed by this file */
#include <hdf5.h>

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Initialize the RADOS VOL connector.
 *
 * Ceph environment variables are read when this is called, so if
 * $CEPH_ARGS specifies everything you need to connect, no further
 * configuration is necessary.
 * If path is NULL, the default locations are searched, and the first
 * found is used. The locations are:
 * - $CEPH_CONF (environment variable)
 * - /etc/ceph/ceph.conf
 * - ~/.ceph/config
 * - ceph.conf (in the current working directory)
 *
 * @param id        [IN]    the user to connect as (i.e. admin, not client.admin)
 * @param path      [IN]    path to a Ceph configuration file
 * @param pool_name [IN]    name of the pool to use
 *
 * @returns 0 on success, negative error code on failure
 */
H5VL_RADOS_PUBLIC herr_t H5VLrados_init(const char * const id, const char *path,
    const char *pool_name);

/**
 * Finalize the RADOS VOL connector.
 *
 * @returns 0 on success, negative error code on failure
 */
H5VL_RADOS_PUBLIC herr_t H5VLrados_term(void);

/**
 * Set the file access property list to use the given MPI communicator/info.
 *
 * @param fapl_id   [IN]    file access property list ID
 * @param comm      [IN]    MPI communicator
 * @param info      [IN]    MPI info
 *
 * @returns 0 on success, negative error code on failure
 */
H5VL_RADOS_PUBLIC herr_t H5Pset_fapl_rados(hid_t fapl_id, MPI_Comm comm, MPI_Info info);

#ifdef __cplusplus
}
#endif

#endif /* H5VLrados_public_H */
