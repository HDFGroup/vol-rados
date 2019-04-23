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
 * Purpose: Implement missing public H5S routines from the HDF5 API.
 */
#ifndef H5VLselect_H
#define H5VLselect_H

#include "H5VLrados_config.h"

#include <hdf5.h>

#ifdef __cplusplus
extern "C" {
#endif

H5VL_RADOS_PRIVATE herr_t H5Shyper_adjust_s(hid_t spaceid, const hssize_t *offset);
H5VL_RADOS_PRIVATE htri_t H5Shyper_intersect_block(hid_t spaceid, const hsize_t *start, const hsize_t *end);
H5VL_RADOS_PRIVATE herr_t H5Sselect_adjust_u(hid_t spaceid, const hsize_t *offset);
H5VL_RADOS_PRIVATE htri_t H5Sselect_shape_same(hid_t space1id, hid_t space2id);
H5VL_RADOS_PRIVATE herr_t H5Sset_extent_real(hid_t space_id, const hsize_t *size);

#ifdef __cplusplus
}
#endif

#endif /* H5VLselect_H */
