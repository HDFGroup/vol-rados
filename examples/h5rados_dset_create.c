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
    hid_t file = -1, dset = -1, space = -1, fapl = -1, dcpl = H5P_DEFAULT;
    hsize_t dims[2] = {4, 6};
    hsize_t cdims[2];

    (void)MPI_Init(&argc, &argv);

    if((argc != 3) && (argc != 5))
        PRINTF_ERROR("argc is not 3 or 5\n");

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

    /* Set up DCPL */
    if(argc == 5) {
        cdims[0] = (hsize_t)atoi(argv[3]);
        cdims[1] = (hsize_t)atoi(argv[4]);
        if((dcpl = H5Pcreate(H5P_DATASET_CREATE)) < 0)
            ERROR;
        if(H5Pset_chunk(dcpl, 2, cdims) < 0)
            ERROR;
    } /* end if */

    /* Set up dataspace */
    if((space = H5Screate_simple(2, dims, NULL)) < 0)
        ERROR;

    /* Open file */
    if((file = H5Fopen(argv[1], H5F_ACC_RDWR, fapl)) < 0)
        ERROR;

    printf("Creating dataset\n");

    /* Create dataset */
    if((dset = H5Dcreate2(file, argv[2], H5T_NATIVE_INT, space, H5P_DEFAULT, dcpl, H5P_DEFAULT)) < 0)
        ERROR;

    /* Close */
    if(H5Dclose(dset) < 0)
        ERROR;
    if(H5Fclose(file) < 0)
        ERROR;
    if(H5Sclose(space) < 0)
        ERROR;
    if(H5Pclose(fapl) < 0)
        ERROR;
    if((dcpl != H5P_DEFAULT) && (H5Pclose(dcpl) < 0))
        ERROR;

    printf("Success\n");

    (void)MPI_Finalize();
    return 0;

error:
    H5E_BEGIN_TRY {
        H5Dclose(dset);
        H5Fclose(file);
        H5Sclose(space);
        H5Pclose(fapl);
    } H5E_END_TRY;

    (void)MPI_Finalize();
    return 1;
}

