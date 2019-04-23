/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 * Copyright by The HDF Group.                                               *
 * All rights reserved.                                                      *
 *                                                                           *
 * This file is part of the HDF5 RADOS VOL connector. The full copyright     *
 * notice, including terms governing use, modification, and redistribution,  *
 * is contained in the COPYING file, which can be found at the root of the   *
 * source code distribution tree.                                            *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

#include "H5VLselect.h"
#include "H5VLerror.h"

/****************/
/* Local Macros */
/****************/

/************************************/
/* Local Type and Struct Definition */
/************************************/

typedef struct H5S_t H5S_t;

/********************/
/* Local Prototypes */
/********************/

herr_t H5S_hyper_adjust_s(H5S_t *space, const hssize_t *offset);
htri_t H5S_hyper_intersect_block(H5S_t *space, const hsize_t *start, const hsize_t *end);
herr_t H5S_select_adjust_u(H5S_t *space, const hsize_t *offset);
htri_t H5S_select_shape_same(const H5S_t *space1, const H5S_t *space2);
herr_t H5S_set_extent_real(H5S_t *space, const hsize_t *size);

/*******************/
/* Local Variables */
/*******************/

/* Error stack declarations */
extern hid_t H5VL_ERR_STACK_g;
extern hid_t H5VL_ERR_CLS_g;

/*---------------------------------------------------------------------------*/
herr_t H5Shyper_adjust_s(hid_t space_id, const hssize_t *offset)
{
    H5S_t *space;

    FUNC_ENTER_VOL(herr_t, SUCCEED)

    if(NULL == (space = (H5S_t *)H5Iobject_verify(space_id, H5I_DATASPACE)))
        HGOTO_ERROR(H5E_DATASPACE, H5E_BADTYPE, FAIL, "not a dataspace");

    if(H5S_hyper_adjust_s(space, offset) < 0)
        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTSET, FAIL, "can't adjust selection");

done:
    FUNC_LEAVE_VOL
}

/*---------------------------------------------------------------------------*/
htri_t
H5Shyper_intersect_block(hid_t space_id, const hsize_t *start, const hsize_t *end)
{
    H5S_t *space;

    FUNC_ENTER_VOL(htri_t, FAIL)

    if(NULL == (space = (H5S_t *)H5Iobject_verify(space_id, H5I_DATASPACE)))
        HGOTO_ERROR(H5E_DATASPACE, H5E_BADTYPE, FAIL, "not a dataspace");

    if(FUNC_RETURN_SET(H5S_hyper_intersect_block(space, start, end)) < 0)
        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTGET, FAIL, "can't determine intersection");

done:
    FUNC_LEAVE_VOL
}

/*---------------------------------------------------------------------------*/
herr_t
H5Sselect_adjust_u(hid_t space_id, const hsize_t *offset)
{
    H5S_t *space;

    FUNC_ENTER_VOL(herr_t, SUCCEED)

    if(NULL == (space = (H5S_t *)H5Iobject_verify(space_id, H5I_DATASPACE)))
        HGOTO_ERROR(H5E_DATASPACE, H5E_BADTYPE, FAIL, "not a dataspace");

    if(H5S_select_adjust_u(space, offset) < 0)
        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTSET, FAIL, "can't adjust selection");

done:
    FUNC_LEAVE_VOL
}

/*---------------------------------------------------------------------------*/
htri_t
H5Sselect_shape_same(hid_t space1_id, hid_t space2_id)
{
    H5S_t *space1, *space2;

    FUNC_ENTER_VOL(htri_t, FAIL)

    if(NULL == (space1 = (H5S_t *)H5Iobject_verify(space1_id, H5I_DATASPACE)))
        HGOTO_ERROR(H5E_DATASPACE, H5E_BADTYPE, FAIL, "not a dataspace");
    if(NULL == (space2 = (H5S_t *)H5Iobject_verify(space2_id, H5I_DATASPACE)))
        HGOTO_ERROR(H5E_DATASPACE, H5E_BADTYPE, FAIL, "not a dataspace");

    if(FUNC_RETURN_SET(H5S_select_shape_same(space1, space2)) < 0)
        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTCOMPARE, FAIL, "can't compare selections");

done:
    FUNC_LEAVE_VOL
}

/*---------------------------------------------------------------------------*/
herr_t
H5Sset_extent_real(hid_t space_id, const hsize_t *size)
{
    H5S_t *space;

    FUNC_ENTER_VOL(herr_t, SUCCEED)

    if(NULL == (space = (H5S_t *)H5Iobject_verify(space_id, H5I_DATASPACE)))
        HGOTO_ERROR(H5E_DATASPACE, H5E_BADTYPE, FAIL, "not a dataspace");

    if(H5S_set_extent_real(space, size) < 0)
        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTSET, FAIL, "can't adjust selection");

done:
    FUNC_LEAVE_VOL
}
