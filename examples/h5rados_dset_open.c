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
    hid_t file = -1, dset = -1, fapl = -1, space = -1, type = -1, dcpl = -1, dapl = -1, def_dcpl = -1, def_dapl = -1;
    int ndims;
    hsize_t dims[2];
    htri_t tri_ret;

    (void)MPI_Init(&argc, &argv);

    if(argc != 3)
        PRINTF_ERROR("argc != 3\n");

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

    /* Open file */
    if((file = H5Fopen(argv[1], H5F_ACC_RDONLY, fapl)) < 0)
        ERROR;

    printf("Opening dataset\n");

    /* Open dataset */
    if((dset = H5Dopen2(file, argv[2], H5P_DEFAULT)) < 0)
        ERROR;

    /* Check dataset dataspace */
    if((space = H5Dget_space(dset)) < 0)
        ERROR;
    if((ndims = H5Sget_simple_extent_ndims(space)) < 0)
        ERROR;
    if(ndims != 2)
        PRINTF_ERROR("ndims == %d, expected 2\n", ndims);
    if(H5Sget_simple_extent_dims(space, dims, NULL) < 0)
        ERROR;
    if(dims[0] != 4)
        PRINTF_ERROR("dims[0] == %d, expected 4\n", (int)dims[0]);
    if(dims[1] != 6)
        PRINTF_ERROR("dims[1] == %d, expected 6\n", (int)dims[1]);

    /* Check dataset datatype */
    if((type = H5Dget_type(dset)) < 0)
        ERROR;
    if((tri_ret = H5Tequal(type, H5T_NATIVE_INT)) < 0)
        ERROR;
    if(!tri_ret)
        PRINTF_ERROR("datatype does not equal H5T_NATIVE_INT\n");

    /* Check DCPL */
    if((dcpl = H5Dget_create_plist(dset)) < 0)
        ERROR;
    if((def_dcpl = H5Pcreate(H5P_DATASET_CREATE)) < 0)
        ERROR;
    if((tri_ret = H5Pequal(dcpl, def_dcpl)) < 0)
        ERROR;
    if(!tri_ret)
        PRINTF_ERROR("DCPL does not equal default\n");

    /* Check DAPL */
    if((dapl = H5Dget_access_plist(dset)) < 0)
        ERROR;
    if((def_dapl = H5Pcreate(H5P_DATASET_ACCESS)) < 0)
        ERROR;
    if((tri_ret = H5Pequal(dapl, def_dapl)) < 0)
        ERROR;
    if(!tri_ret)
        PRINTF_ERROR("DAPL does not equal default\n");

    /* Close */
    if(H5Dclose(dset) < 0)
        ERROR;
    if(H5Fclose(file) < 0)
        ERROR;
    if(H5Pclose(fapl) < 0)
        ERROR;
    if(H5Sclose(space) < 0)
        ERROR;
    if(H5Tclose(type) < 0)
        ERROR;
    if(H5Pclose(dcpl) < 0)
        ERROR;
    if(H5Pclose(dapl) < 0)
        ERROR;
    if(H5Pclose(def_dcpl) < 0)
        ERROR;
    if(H5Pclose(def_dapl) < 0)
        ERROR;

    printf("Success\n");

    (void)MPI_Finalize();
    return 0;

error:
    H5E_BEGIN_TRY {
        H5Dclose(dset);
        H5Fclose(file);
        H5Pclose(fapl);
        H5Sclose(space);
        H5Tclose(type);
        H5Pclose(dcpl);
        H5Pclose(dapl);
        H5Pclose(def_dcpl);
        H5Pclose(def_dapl);
    } H5E_END_TRY;

    (void)MPI_Finalize();
    return 1;
}

