/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 * Copyright by The HDF Group.                                               *
 * Copyright by the Board of Trustees of the University of Illinois.         *
 * All rights reserved.                                                      *
 *                                                                           *
 * This file is part of HDF5.  The full HDF5 copyright notice, including     *
 * terms governing use, modification, and redistribution, is contained in    *
 * the COPYING file, which can be found at the root of the source code       *
 * distribution tree, or in https://support.hdfgroup.org/ftp/HDF5/releases.  *
 * If you do not have access to either file, you may request a copy from     *
 * help@hdfgroup.org.                                                        *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

/* This source code was developed under the Mochi project
 * (https://www.mcs.anl.gov/research/projects/mochi), supported by the U.S.
 * Department of Energy, Office of Science, under contract DE-AC02-06CH11357.
 */

#include "h5rados_example.h"

int main(int argc, char *argv[]) {
    char *pool = "mypool";
    hid_t file = -1, fapl = -1;

    (void)MPI_Init(&argc, &argv);

    if(argc != 2)
        PRINTF_ERROR("argc != 2\n");

    /* Initialize VOL */
    if(H5VLrados_init(NULL, CEPH_CONFIG_FILE, pool) < 0)
        ERROR;

    /* Set up FAPL */
    if((fapl = H5Pcreate(H5P_FILE_ACCESS)) < 0)
        ERROR;
    if(H5Pset_fapl_rados(fapl, MPI_COMM_WORLD, MPI_INFO_NULL) < 0)
        ERROR;
    if(H5Pset_all_coll_metadata_ops(fapl, true) < 0)
        ERROR;

    /* Create file */
    if((file = H5Fcreate(argv[1], H5F_ACC_TRUNC, H5P_DEFAULT, fapl)) < 0)
        ERROR;

    /* Close */
    if(H5Fclose(file) < 0)
        ERROR;
    if(H5Pclose(fapl) < 0)
        ERROR;

    printf("Success\n");

    (void)MPI_Finalize();
    return 0;

error:
    H5E_BEGIN_TRY {
        H5Fclose(file);
        H5Pclose(fapl);
    } H5E_END_TRY;

    (void)MPI_Finalize();
    return 1;
}

