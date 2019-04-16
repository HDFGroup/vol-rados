/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 * Copyright by The HDF Group.                                               *
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

/*
 * Programmer:  Neil Fortner <nfortne2@hdfgroup.org>
 *              December, 2017
 *
 * Purpose: The RADOS VOL plugin where access is forwarded to the RADOS API 
 */
 
#define H5O_FRIEND              /* Suppress error about including H5Opkg */

#include "H5private.h"          /* Generic Functions                    */
#include "H5Dprivate.h"         /* Datasets                             */
#include "H5Eprivate.h"         /* Error handling                       */
#include "H5Fprivate.h"         /* Files                                */
#include "H5FDprivate.h"        /* File drivers                         */
#include "H5Gprivate.h"         /* Groups                               */
#include "H5Iprivate.h"         /* IDs                                  */
#include "H5MMprivate.h"        /* Memory management                    */
#include "H5Opkg.h"             /* Objects                              */
#include "H5Pprivate.h"         /* Property lists                       */
#include "H5Sprivate.h"         /* Dataspaces                           */
#include "H5VLprivate.h"        /* VOL plugins                          */
#include "H5VLrados.h"          /* RADOS plugin                         */

hid_t H5VL_RADOS_g = -1;

/*
 * Macros
 */
/* Stack allocation sizes */
#define H5VL_RADOS_FOI_BUF_SIZE 1024
#define H5VL_RADOS_LINK_VAL_BUF_SIZE 256
#define H5VL_RADOS_GINFO_BUF_SIZE 256
#define H5VL_RADOS_DINFO_BUF_SIZE 1024
#define H5VL_RADOS_SEQ_LIST_LEN 128

/* Definitions for building oids */
#define H5VL_RADOS_IDX_MASK   0x3fffffffffffffffull
#define H5VL_RADOS_TYPE_GRP   0x0000000000000000ull
#define H5VL_RADOS_TYPE_DSET  0x4000000000000000ull
#define H5VL_RADOS_TYPE_DTYPE 0x8000000000000000ull

/* Definitions for chunking code */
#define H5VL_RADOS_DEFAULT_NUM_SEL_CHUNKS 64

/*
 * Typedefs
 */
/* RADOS-specific file access properties */
typedef struct H5VL_rados_info_t {
    MPI_Comm            comm;           /*communicator                  */
    MPI_Info            info;           /*file information              */
} H5VL_rados_info_t;

/* Enum to indicate if the supplied read buffer can be used as a type conversion
 * or background buffer */
typedef enum {
    H5VL_RADOS_TCONV_REUSE_NONE,    /* Cannot reuse buffer */
    H5VL_RADOS_TCONV_REUSE_TCONV,   /* Use buffer as type conversion buffer */
    H5VL_RADOS_TCONV_REUSE_BKG      /* Use buffer as background buffer */
} H5VL_rados_tconv_reuse_t;

/* Udata type for H5Dscatter callback */
typedef struct H5VL_rados_scatter_cb_ud_t {
    void *buf;
    size_t len;
} H5VL_rados_scatter_cb_ud_t;


/* Information about a singular selected chunk during a Dataset read/write */
typedef struct H5VL_rados_select_chunk_info_t {
    uint64_t chunk_coords[H5S_MAX_RANK]; /* The starting coordinates ("upper left corner") of the chunk */
    hid_t    mspace_id;                  /* The memory space corresponding to the
                                            selection in the chunk in memory */
    hid_t    fspace_id;                  /* The file space corresponding to the
                                            selection in the chunk in the file */
} H5VL_rados_select_chunk_info_t;

/*
 * Prototypes
 */
static void *H5VL_rados_info_copy(const void *_old_info);
static herr_t H5VL_rados_info_cmp(int *cmp_value, const void *_info1, const void *_info2);
static herr_t H5VL_rados_info_free(void *_info);
static herr_t H5VL_rados_term(void);

/* File callbacks */
static void *H5VL_rados_file_create(const char *name, unsigned flags,
    hid_t fcpl_id, hid_t fapl_id, hid_t dxpl_id, void **req);
static void *H5VL_rados_file_open(const char *name, unsigned flags,
    hid_t fapl_id, hid_t dxpl_id, void **req);
static herr_t H5VL_rados_file_specific(void *_item,
    H5VL_file_specific_t specific_type, hid_t dxpl_id, void **req,
    va_list arguments);
static herr_t H5VL_rados_file_close(void *_file, hid_t dxpl_id, void **req);

/* Link callbacks */

/* Group callbacks */
static void *H5VL_rados_group_create(void *_item, const H5VL_loc_params_t *loc_params,
    const char *name, hid_t gcpl_id, hid_t gapl_id, hid_t dxpl_id, void **req);
static void *H5VL_rados_group_open(void *_item, const H5VL_loc_params_t *loc_params,
    const char *name, hid_t gapl_id, hid_t dxpl_id, void **req);
static herr_t H5VL_rados_group_close(void *_grp, hid_t dxpl_id, void **req);

/* Dataset callbacks */
static void *H5VL_rados_dataset_create(void *_item,
    const H5VL_loc_params_t *loc_params, const char *name, hid_t dcpl_id,
    hid_t dapl_id, hid_t dxpl_id, void **req);
static void *H5VL_rados_dataset_open(void *_item, const H5VL_loc_params_t *loc_params,
    const char *name, hid_t dapl_id, hid_t dxpl_id, void **req);
static herr_t H5VL_rados_dataset_read(void *_dset, hid_t mem_type_id,
    hid_t mem_space_id, hid_t file_space_id, hid_t dxpl_id, void *buf,
    void **req);
static herr_t H5VL_rados_dataset_write(void *_dset, hid_t mem_type_id,
    hid_t mem_space_id, hid_t file_space_id, hid_t dxpl_id, const void *buf,
    void **req);
static herr_t H5VL_rados_dataset_get(void *_dset, H5VL_dataset_get_t get_type,
    hid_t dxpl_id, void **req, va_list arguments);
static herr_t H5VL_rados_dataset_close(void *_dset, hid_t dxpl_id, void **req);

/* Datatype callbacks */

/* Object callbacks */

/* Attribute callbacks */

/* Helper routines */
static herr_t H5VL_rados_write_max_oid(H5VL_rados_file_t *file);
static herr_t H5VL_rados_file_flush(H5VL_rados_file_t *file);
static herr_t H5VL_rados_file_close_helper(H5VL_rados_file_t *file, hid_t dxpl_id, void **req);

/* read/write_op equivalents for some RADOS calls */
static int H5VL_rados_read(rados_ioctx_t io, const char *oid, char *buf, size_t len, uint64_t off);
static int H5VL_rados_write_full(rados_ioctx_t io, const char *oid, const char *buf, size_t len);
static int H5VL_rados_stat(rados_ioctx_t io, const char *oid, uint64_t *psize, time_t * pmtime);

static herr_t H5VL_rados_link_read(H5VL_rados_group_t *grp, const char *name, H5VL_rados_link_val_t *val);
static herr_t H5VL_rados_link_write(H5VL_rados_group_t *grp, const char *name, H5VL_rados_link_val_t *val);
static herr_t H5VL_rados_link_follow(H5VL_rados_group_t *grp, const char *name, hid_t dxpl_id, void **req, uint64_t *oid);
static herr_t H5VL_rados_link_follow_comp(H5VL_rados_group_t *grp, char *name, size_t name_len, hid_t dxpl_id, void **req, uint64_t *oid);

static H5VL_rados_group_t *H5VL_rados_group_traverse(H5VL_rados_item_t *item,
    char *path, hid_t dxpl_id, void **req, char **obj_name,
    void **gcpl_buf_out, uint64_t *gcpl_len_out);
static H5VL_rados_group_t *H5VL_rados_group_traverse_const(
    H5VL_rados_item_t *item, const char *path, hid_t dxpl_id, void **req,
    const char **obj_name, void **gcpl_buf_out, uint64_t *gcpl_len_out);
static void *H5VL_rados_group_create_helper(H5VL_rados_file_t *file,
    hid_t gcpl_id, hid_t gapl_id, hid_t dxpl_id, void **req,
    H5VL_rados_group_t *parent_grp, const char *name, hbool_t collective);
static void *H5VL_rados_group_open_helper(H5VL_rados_file_t *file,
    uint64_t oid, hid_t gapl_id, hid_t dxpl_id, void **req, void **gcpl_buf_out,
    uint64_t *gcpl_len_out);
static void *H5VL_rados_group_reconstitute(H5VL_rados_file_t *file,
    uint64_t oid, uint8_t *gcpl_buf, hid_t gapl_id, hid_t dxpl_id, void **req);

static htri_t H5VL_rados_need_bkg(hid_t src_type_id, hid_t dst_type_id,
    size_t *dst_type_size, hbool_t *fill_bkg);
static herr_t H5VL_rados_tconv_init(hid_t src_type_id, size_t *src_type_size,
    hid_t dst_type_id, size_t *dst_type_size, hbool_t *_types_equal,
    H5VL_rados_tconv_reuse_t *reuse, hbool_t *_need_bkg, hbool_t *fill_bkg);
static herr_t H5VL_rados_get_selected_chunk_info(hid_t dcpl_id,
    hid_t file_space_id, hid_t mem_space_id,
    H5VL_rados_select_chunk_info_t **chunk_info, size_t *chunk_info_len);
static herr_t H5VL_rados_build_io_op_merge(H5S_t *mem_space, H5S_t *file_space,
    size_t type_size, size_t tot_nelem, void *rbuf, const void *wbuf,
    rados_read_op_t read_op, rados_write_op_t write_op);
static herr_t H5VL_rados_build_io_op_match(H5S_t *file_space, size_t type_size,
    size_t tot_nelem, void *rbuf, const void *wbuf, rados_read_op_t read_op,
    rados_write_op_t write_op);
static herr_t H5VL_rados_build_io_op_contig(H5S_t *file_space, size_t type_size,
    size_t tot_nelem, void *rbuf, const void *wbuf, rados_read_op_t read_op,
    rados_write_op_t write_op);
static herr_t H5VL_rados_scatter_cb(const void **src_buf,
    size_t *src_buf_bytes_used, void *_udata);

/* Free list definitions */
H5FL_DEFINE(H5VL_rados_file_t);
H5FL_DEFINE(H5VL_rados_group_t);
H5FL_DEFINE(H5VL_rados_dset_t);

/* The RADOS VOL plugin struct */
static H5VL_class_t H5VL_rados_g = {
    H5VL_RADOS_VERSION,                         /* version      */
    H5VL_RADOS_VALUE,                           /* value        */
    H5VL_RADOS_NAME,                            /* name         */
    0,                                          /* capability flags */
    NULL,                                       /* initialize */
    H5VL_rados_term,                            /* terminate */
    {   /* info_cls - may need more here (DER) */
        sizeof(H5VL_rados_info_t),                  /* info size    */
        H5VL_rados_info_copy,                       /* info copy    */
        H5VL_rados_info_cmp,                        /* info compare */
        H5VL_rados_info_free,                       /* info free    */
        NULL,                                       /* info to str  */
        NULL                                        /* str to info  */
    },
    {   /* wrap_cls */
        NULL,                                       /* get_object   */
        NULL,                                       /* get_wrap_ctx */
        NULL,                                       /* wrap_object  */
        NULL                                        /* free_wrap_ctx */
    },
    {                                           /* attribute_cls */
        NULL,//H5VL_rados_attribute_create,            /* create */
        NULL,//H5VL_rados_attribute_open,              /* open */
        NULL,//H5VL_rados_attribute_read,              /* read */
        NULL,//H5VL_rados_attribute_write,             /* write */
        NULL,//H5VL_rados_attribute_get,               /* get */
        NULL,//H5VL_rados_attribute_specific,          /* specific */
        NULL,                                   /* optional */
        NULL,//H5VL_rados_attribute_close              /* close */
    },
    {                                           /* dataset_cls */
        H5VL_rados_dataset_create,              /* create */
        H5VL_rados_dataset_open,                /* open */
        H5VL_rados_dataset_read,                /* read */
        H5VL_rados_dataset_write,               /* write */
        H5VL_rados_dataset_get,                 /* get */
        NULL,//H5VL_iod_dataset_specific,              /* specific */
        NULL,                                   /* optional */
        H5VL_rados_dataset_close                /* close */
    },
    {                                           /* datatype_cls */
        NULL,//H5VL_rados_datatype_commit,             /* commit */
        NULL,//H5VL_rados_datatype_open,               /* open */
        NULL,//H5VL_rados_datatype_get,                /* get */
        NULL,                                   /* specific */
        NULL,                                   /* optional */
        NULL,//H5VL_rados_datatype_close               /* close */
    },
    {                                           /* file_cls */
        H5VL_rados_file_create,                 /* create */
        H5VL_rados_file_open,                   /* open */
        NULL,//H5VL_iod_file_get,                      /* get */
        H5VL_rados_file_specific,               /* specific */
        NULL,                                   /* optional */
        H5VL_rados_file_close                   /* close */
    },
    {                                           /* group_cls */
        H5VL_rados_group_create,                /* create */
        H5VL_rados_group_open,                  /* open */
        NULL,//H5VL_iod_group_get,                     /* get */
        NULL,                                   /* specific */
        NULL,                                   /* optional */
        H5VL_rados_group_close                  /* close */
    },
    {                                           /* link_cls */
        NULL,//H5VL_rados_link_create,                 /* create */
        NULL,//H5VL_iod_link_copy,                     /* copy */
        NULL,//H5VL_iod_link_move,                     /* move */
        NULL,//H5VL_iod_link_get,                      /* get */
        NULL,//H5VL_rados_link_specific,               /* specific */
        NULL                                    /* optional */
    },
    {                                           /* object_cls */
        NULL,//H5VL_rados_object_open,                 /* open */
        NULL,                                   /* copy */
        NULL,                                   /* get */
        NULL,//H5VL_iod_object_specific,               /* specific */
        NULL,//H5VL_rados_object_optional              /* optional */
    },
    {   /* request_cls */
        NULL,                                       /* wait         */
        NULL,                                       /* notify       */
        NULL,                                       /* cancel       */
        NULL,                                       /* specific     */
        NULL,                                       /* optional     */
        NULL                                        /* free         */
    },
    NULL                                            /* optional     */
};

/* The RADOS cluster */
rados_t cluster_g;
hbool_t cluster_init_g = FALSE;

/* The RADOS IO context */
rados_ioctx_t ioctx_g;
hbool_t ioctx_init_g = FALSE;


/*-------------------------------------------------------------------------
 * Function:    H5VL_rados_read
 *
 * Purpose:     Recreates rados_read(). We are trying to use all read_op
 *              calls and this lets us do that without duplicating all
 *              the RADOS boilerplate.
 *
 * Return:      Success:        0
 *              Failure:        -1
 *
 * Programmer:  Dana Robinson
 *              February, 2019
 *
 *-------------------------------------------------------------------------
 */
static int
H5VL_rados_read(rados_ioctx_t io, const char *oid, char *buf, size_t len, uint64_t off)
{
    int ret = 0;
    int ret_value = 0;

    FUNC_ENTER_NOAPI_NOINIT

#ifdef OLD_RADOS_CALLS
    /* Read data from the object */
    if((ret = rados_read(io, oid, buf, len, off)) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTDECODE, (-1), "can't read metadata from dataset: %s", strerror(-ret))
#else
{
    rados_read_op_t read_op;
    hbool_t read_op_init = FALSE;
    size_t bytes_read = 0;
    int prval;

    /* Create read op */
    if(NULL == (read_op = rados_create_read_op()))
        HGOTO_ERROR(H5E_SYM, H5E_CANTINIT, (-1), "can't create read operation")
    read_op_init = TRUE;

    /* Add the read operation (returns void) */
    rados_read_op_read(read_op, off, len, buf, &bytes_read, &prval);

    /* Execute read operation */
    if((ret = rados_read_op_operate(read_op, io, oid, LIBRADOS_OPERATION_NOFLAG)) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTINIT, (-1), "can't perform read operation: %s", strerror(-ret))
    if(0 < prval)
        HGOTO_ERROR(H5E_SYM, H5E_NOTFOUND, (-1), "RADOS read operation failed for object")
    if(0 == bytes_read)
        HGOTO_ERROR(H5E_SYM, H5E_NOTFOUND, (-1), "metadata not found for object")

    /* clean up */
    if(read_op_init)
        rados_release_read_op(read_op);
}
#endif

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5VL_rados_read() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_rados_write_full
 *
 * Purpose:     Recreates rados_write_full(). We are trying to use all
 *              write_op calls and this lets us do that without duplicating
 *              all the RADOS boilerplate.
 *
 * Return:      Success:        0
 *              Failure:        -1
 *
 * Programmer:  Dana Robinson
 *              February, 2019
 *
 *-------------------------------------------------------------------------
 */
static int
H5VL_rados_write_full(rados_ioctx_t io, const char *oid, const char *buf, size_t len)
{
    int ret = 0;
    int ret_value = 0;

    FUNC_ENTER_NOAPI_NOINIT

#ifdef OLD_RADOS_CALLS
    if((ret = rados_write_full(io, oid, buf, len)) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTINIT, (-1), "can't write metadata to object: %s", strerror(-ret))
#else
{
    rados_write_op_t write_op;
    hbool_t write_op_init = FALSE;

    /* Create write op */
    if(NULL == (write_op = rados_create_write_op()))
        HGOTO_ERROR(H5E_SYM, H5E_CANTINIT, (-1), "can't create write operation")
    write_op_init = TRUE;

    /* Add the write full operation (returns void) */
    rados_write_op_write_full(write_op, buf, len);

    /* Execute write operation */
    if((ret = rados_write_op_operate(write_op, io, oid, NULL, LIBRADOS_OPERATION_NOFLAG)) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTINIT, (-1), "can't perform write operation: %s", strerror(-ret))

    /* clean up */
    if(write_op_init)
        rados_release_write_op(write_op);
}
#endif

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5VL_rados_write_full() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_rados_stat
 *
 * Purpose:     Recreates rados_stat(). We are trying to use all read_op
 *              calls and this lets us do that without duplicating all
 *              the RADOS boilerplate.
 *
 * Return:      Success:        0
 *              Failure:        -1
 *
 * Programmer:  Dana Robinson
 *              February, 2019
 *
 *-------------------------------------------------------------------------
 */
static int
H5VL_rados_stat(rados_ioctx_t io, const char *oid, uint64_t *psize, time_t * pmtime)
{
    int ret = 0;
    int ret_value = 0;

    FUNC_ENTER_NOAPI_NOINIT

#ifdef OLD_RADOS_CALLS
    /* Get the object size and time */
    if((ret = rados_stat(io, oid, psize, pmtime)) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTDECODE, (-1), "can't read object size and time: %s", strerror(-ret))
#else
{
    rados_read_op_t read_op;
    hbool_t read_op_init = FALSE;
    int prval;

    /* Create read op */
    if(NULL == (read_op = rados_create_read_op()))
        HGOTO_ERROR(H5E_SYM, H5E_CANTINIT, (-1), "can't create read operation")
    read_op_init = TRUE;

    /* Add the get stats operation (returns void) */
    rados_read_op_stat(read_op, psize, pmtime, &prval);

    /* Execute read operation */
    if((ret = rados_read_op_operate(read_op, io, oid, LIBRADOS_OPERATION_NOFLAG)) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTINIT, (-1), "can't perform read operation: %s", strerror(-ret))
    if(0 < prval)
        HGOTO_ERROR(H5E_SYM, H5E_NOTFOUND, (-1), "stats not found for object")

    /* clean up */
    if(read_op_init)
        rados_release_read_op(read_op);
}
#endif

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5VL_rados_stat() */


/* Create a RADOS string oid given the file name and binary oid */
static herr_t
H5VL_rados_oid_create_string_name(const char *file_name, size_t file_name_len,
    uint64_t bin_oid, char **oid)
{
    char *tmp_oid = NULL;
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_NOAPI_NOINIT

    /* Allocate space for oid */
    if(NULL == (tmp_oid = (char *)H5MM_malloc(2 + file_name_len + 16 + 1)))
        HGOTO_ERROR(H5E_VOL, H5E_CANTALLOC, FAIL, "can't allocate RADOS object id")

    /* Encode file name and binary oid into string oid */
    if(HDsnprintf(tmp_oid, 2 + file_name_len + 16 + 1, "ob%s%016llX",
            file_name, (long long unsigned)bin_oid)
            != 2 + (int)file_name_len + 16)
        HGOTO_ERROR(H5E_VOL, H5E_CANTINIT, FAIL, "can't encode string object id")

    /* Return oid string value */
    *oid = tmp_oid;
    tmp_oid = NULL;

done:
    H5MM_xfree(tmp_oid);

    FUNC_LEAVE_NOAPI(ret_value)
}


/* Create a RADOS string oid given the file name and binary oid */
static herr_t
H5VL_rados_oid_create_string(const H5VL_rados_file_t *file, uint64_t bin_oid,
    char **oid)
{
    return H5VL_rados_oid_create_string_name(file->file_name,
        file->file_name_len, bin_oid, oid);
} /* end H5VL_rados_oid_create_string() */


/* Create a RADOS string oid for a data chunk given the file name, binary oid,
 * dataset rank, and chunk location. If *oid is not NULL, it is assumed to be a
 * buffer large enough, i.e. one previously returned by this function with the
 * same file and rank */
static herr_t
H5VL_rados_oid_create_chunk(const H5VL_rados_file_t *file, uint64_t bin_oid,
    int rank, uint64_t *chunk_loc, char **oid)
{
    char *tmp_buf = NULL;
    char *enc_buf = NULL;
    size_t oid_len;
    size_t oid_off;
    int i;
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_NOAPI_NOINIT

    HDassert((rank >= 0) && (rank <= 99));

    /* Calculate space needed for oid */
    oid_len = 2 + file->file_name_len + 16 + ((size_t)rank * 16) + 1;

    /* Assign encoding buffer and allocate buffer, if needed */
    if(*oid)
        enc_buf = *oid;
    else {
        if(NULL == (tmp_buf = (char *)H5MM_malloc(oid_len)))
            HGOTO_ERROR(H5E_VOL, H5E_CANTALLOC, FAIL, "can't allocate RADOS object id")
        enc_buf = tmp_buf;
    } /* end else */

    /* Encode file name and binary oid into string oid */
    if(HDsnprintf(enc_buf, oid_len, "%02d%s%016llX", rank, file->file_name,
            (long long unsigned)bin_oid) != 2 + (int)file->file_name_len + 16)
        HGOTO_ERROR(H5E_VOL, H5E_CANTINIT, FAIL, "can't encode string object id")
    oid_off = 2 + file->file_name_len + 16;

    /* Encode chunk location */
    for(i = 0; i < rank; i++) {
        if(HDsnprintf(enc_buf + oid_off, oid_len - oid_off, "%016llX", (long long unsigned)chunk_loc[i])
                != 16)
            HGOTO_ERROR(H5E_VOL, H5E_CANTINIT, FAIL, "can't encode string object id")
        oid_off += 16;
    } /* end for */

    /* Return oid string value */
    if(!*oid) {
        *oid = tmp_buf;
        tmp_buf = NULL;
    } /* end if */

done:
    H5MM_xfree(tmp_buf);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5VL_rados_oid_create_chunk() */


/* Create a binary RADOS oid given the object type and a 64 bit index (top 2
 * bits are ignored) */
static void
H5VL_rados_oid_create_binary(uint64_t idx, H5I_type_t obj_type,
    uint64_t *bin_oid)
{
    /* Initialize bin_oid */
    *bin_oid = idx & H5VL_RADOS_IDX_MASK;

    /* Set type_bits */
    if(obj_type == H5I_GROUP)
        *bin_oid |= H5VL_RADOS_TYPE_GRP;
    else if(obj_type == H5I_DATASET)
        *bin_oid |= H5VL_RADOS_TYPE_DSET;
    else {
        HDassert(obj_type == H5I_DATATYPE);
        *bin_oid |= H5VL_RADOS_TYPE_DTYPE;
    } /* end else */

    return;
} /* end H5VL_rados_oid_create_binary() */


/* Create a RADOS oid given the file name, object type and a 64 bit index (top 2
 * bits are ignored) */
static herr_t
H5VL_rados_oid_create(const H5VL_rados_file_t *file, uint64_t idx,
    H5I_type_t obj_type, uint64_t *bin_oid, char **oid)
{
    uint64_t tmp_bin_oid = *bin_oid;
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_NOAPI_NOINIT

    /* Create binary oid */
    H5VL_rados_oid_create_binary(idx, obj_type, &tmp_bin_oid);

    /* Create sting oid */
    if(H5VL_rados_oid_create_string(file, tmp_bin_oid, oid) < 0)
        HGOTO_ERROR(H5E_VOL, H5E_CANTINIT, FAIL, "can't encode string object id")

    /* Return oid binary value (string already returned) */
    *bin_oid = tmp_bin_oid;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5VL_rados_oid_create() */


/* Retrieve the 64 bit object index from a RADOS oid  */
static uint64_t
H5VL_rados_oid_to_idx(uint64_t bin_oid)
{
    return bin_oid & H5VL_RADOS_IDX_MASK;
} /* end H5VL_rados_oid_to_idx() */


/*-------------------------------------------------------------------------
 * Function:    H5VLrados_init
 *
 * Purpose:     Initialize this vol plugin by connecting to the cluster
 *              and registering the driver with the library.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 * Programmer:  Neil Fortner
 *              February, 2018
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5VLrados_init(rados_t rados_cluster, const char *rados_pool)
{
    int ret;
    herr_t ret_value = SUCCEED;            /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE2("e", "x*s", rados_cluster, rados_pool);

    /* Check if already initialized */
    if(H5VL_RADOS_g >= 0)
        HGOTO_DONE(SUCCEED)

    /* Register the RADOS VOL, if it isn't already */
    if(H5VL_rados_init() < 0)
        HGOTO_ERROR(H5E_VOL, H5E_CANTINIT, FAIL, "unable to initialize RADOS VOL plugin")

    /* Save cluster */
    cluster_g = rados_cluster;

    /* Connect to cluster */
    if((ret = rados_connect(rados_cluster)) < 0)
        HGOTO_ERROR(H5E_VOL, H5E_CANTINIT, FAIL, "can't connect to cluster: %s", strerror(-ret))
    cluster_init_g = TRUE;

    /* Create IO context */
    if((ret = rados_ioctx_create(cluster_g, rados_pool, &ioctx_g)) < 0)
        HGOTO_ERROR(H5E_VOL, H5E_CANTINIT, FAIL, "can't create IO context: %s", strerror(-ret))
    ioctx_init_g = TRUE;

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5VLrados_init() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_rados_init
 *
 * Purpose:     Initialize this vol plugin by registering the driver with the
 *              library.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 * Programmer:  Neil Fortner
 *              February, 2018
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5VL_rados_init(void)
{
    herr_t ret_value = SUCCEED;            /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Register interfaces that might not be initialized in time (for example if
     * we open an object without knowing its type first, H5Oopen will not
     * initialize that type) */
    if(H5D_init() < 0)
        HGOTO_ERROR(H5E_FUNC, H5E_CANTINIT, FAIL, "unable to initialize dataset interface")
    if(H5T_init() < 0)
        HGOTO_ERROR(H5E_FUNC, H5E_CANTINIT, FAIL, "unable to initialize datatype interface")

    /* Register the RADOS VOL, if it isn't already */
    if(NULL == H5I_object_verify(H5VL_RADOS_g, H5I_VOL)) {
        if((H5VL_RADOS_g = H5VL_register_connector((const H5VL_class_t *)&H5VL_rados_g, TRUE, H5P_DEFAULT)) < 0)
            HGOTO_ERROR(H5E_ATOM, H5E_CANTINSERT, FAIL, "can't create ID for RADOS plugin")
    } /* end if */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5VL_rados_init() */


/*-------------------------------------------------------------------------
 * Function:    H5VLrados_term
 *
 * Purpose:     Shut down the RADOS VOL
 *
 * Return:      Non-negative on success/Negative on failure
 *
 * Programmer:  Neil Fortner
 *              January, 2018
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5VLrados_term(void)
{
    herr_t ret_value = SUCCEED;            /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE0("e","");

    /* Terminate the plugin */
    if(H5VL_rados_term() < 0)
        HGOTO_ERROR(H5E_VOL, H5E_CLOSEERROR, FAIL, "can't close RADOS VOL plugin")

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5VLrados_term() */


/*---------------------------------------------------------------------------
 * Function:    H5VL_rados_term
 *
 * Purpose:     Shut down the RADOS VOL
 *
 * Returns:     Non-negative on success/Negative on failure
 *
 *---------------------------------------------------------------------------
 */
static herr_t
H5VL_rados_term(void)
{
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_NOAPI_NOINIT_NOERR

    if(H5VL_RADOS_g >= 0) {
        /* Terminate RADOS */
        if(ioctx_init_g) {
            rados_ioctx_destroy(ioctx_g);
            ioctx_init_g = FALSE;
        } /* end if */
        if(cluster_init_g) {
            rados_shutdown(cluster_g);
            cluster_init_g = FALSE;
        } /* end if */

        /* "Forget" plugin id.  This should normally be called by the library
         * when it is closing the id, so no need to close it here. */
        H5VL_RADOS_g = -1;
    } /* end if */

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5VL_rados_term() */


/*-------------------------------------------------------------------------
 * Function:    H5Pset_fapl_rados
 *
 * Purpose:     Modify the file access property list to use the H5VL_RADOS
 *              plugin defined in this source file.  file_comm and
 *              file_info identify the communicator and info object used
 *              to coordinate actions on file create, open, flush, and
 *              close.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 * Programmer:  Neil Fortner
 *              January, 2018
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Pset_fapl_rados(hid_t fapl_id, MPI_Comm file_comm, MPI_Info file_info)
{
    H5VL_rados_info_t info;
    H5P_genplist_t  *plist;      /* Property list pointer */
    herr_t          ret_value;

    FUNC_ENTER_API(FAIL)
    H5TRACE3("e", "iMcMi", fapl_id, file_comm, file_info);

    if(H5VL_RADOS_g < 0)
        HGOTO_ERROR(H5E_VOL, H5E_UNINITIALIZED, FAIL, "RADOS VOL plugin not initialized")

    if(fapl_id == H5P_DEFAULT)
        HGOTO_ERROR(H5E_PLIST, H5E_BADVALUE, FAIL, "can't set values in default property list")

    if(NULL == (plist = H5P_object_verify(fapl_id, H5P_FILE_ACCESS)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a file access property list")

    if(MPI_COMM_NULL == file_comm)
        HGOTO_ERROR(H5E_PLIST, H5E_BADTYPE, FAIL, "not a valid communicator")

    /* Initialize driver specific properties */
    info.comm = file_comm;
    info.info = file_info;

    ret_value = H5P_set_vol(plist, H5VL_RADOS_g, &info);

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Pset_fapl_rados() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_rados_info_copy
 *
 * Purpose:     Copies the rados-specific file access properties.
 *
 * Return:      Success:        Ptr to a new property list
 *              Failure:        NULL
 *
 * Programmer:  Neil Fortner
 *              February, 2018
 *
 *-------------------------------------------------------------------------
 */
static void *
H5VL_rados_info_copy(const void *_old_info)
{
    const H5VL_rados_info_t *old_info = (const H5VL_rados_info_t *)_old_info;
    H5VL_rados_info_t       *new_info = NULL;
    void                    *ret_value = NULL;

    FUNC_ENTER_NOAPI_NOINIT

    if(NULL == (new_info = (H5VL_rados_info_t *)H5MM_malloc(sizeof(H5VL_rados_info_t))))
        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, NULL, "memory allocation failed")

    /* Copy the general information */
    HDmemcpy(new_info, old_info, sizeof(H5VL_rados_info_t));

    /* Clear allocated fields, so they aren't freed if something goes wrong.  No
     * need to clear info since it is only freed if comm is not null. */
    new_info->comm = MPI_COMM_NULL;

    /* Duplicate communicator and Info object. */
    if(FAIL == H5FD_mpi_comm_info_dup(old_info->comm, old_info->info, &new_info->comm, &new_info->info))
        HGOTO_ERROR(H5E_INTERNAL, H5E_CANTCOPY, NULL, "Communicator/Info duplicate failed")

    ret_value = new_info;

done:
    if(NULL == ret_value) {
        /* cleanup */
        if(new_info && H5VL_rados_info_free(new_info) < 0)
            HDONE_ERROR(H5E_PLIST, H5E_CANTFREE, NULL, "can't free fapl")
    }

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5VL_rados_info_copy() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_rados_info_cmp
 *
 * Purpose:     Compare rados-specific file access properties.
 *
 * Return:      Success:    0, cmp_value set to a value like strcmp
 *              Failure:    -1
 *
 * Programmer:  Dana Robinson
 *              April, 2019
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_rados_info_cmp(int *cmp_value, const void *_info1, const void *_info2)
{
    H5VL_rados_info_t   *info1 = (H5VL_rados_info_t *)_info1;
    H5VL_rados_info_t   *info2 = (H5VL_rados_info_t *)_info2;
    herr_t              ret_value = SUCCEED;

    FUNC_ENTER_NOAPI_NOINIT

    assert(info1);
    assert(info2);

    *cmp_value = memcmp(info1, info2, sizeof(H5VL_rados_info_t));

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5VL_rados_info_cmp() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_rados_info_free
 *
 * Purpose:     Frees the rados-specific file access properties.
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 * Programmer:  Neil Fortner
 *              February, 2018
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_rados_info_free(void *_info)
{
    herr_t              ret_value = SUCCEED;
    H5VL_rados_info_t   *info = (H5VL_rados_info_t *)_info;

    FUNC_ENTER_NOAPI_NOINIT

    assert(info);

    /* Free the internal communicator and INFO object */
    if(info->comm != MPI_COMM_NULL)
        if(H5FD_mpi_comm_info_free(&info->comm, &info->info) < 0)
            HGOTO_ERROR(H5E_INTERNAL, H5E_CANTFREE, FAIL, "Communicator/Info free failed")

    /* free the struct */
    H5MM_xfree(info);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5VL_rados_info_free() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_rados_file_create
 *
 * Purpose:     Creates a file as a rados HDF5 file.
 *
 * Return:      Success:        the file id. 
 *              Failure:        NULL
 *
 * Programmer:  Neil Fortner
 *              January, 2018
 *
 *-------------------------------------------------------------------------
 */
static void *
H5VL_rados_file_create(const char *name, unsigned flags, hid_t fcpl_id,
    hid_t fapl_id, hid_t dxpl_id, void **req)
{
    H5VL_rados_info_t *info = NULL;
    H5P_genplist_t *plist = NULL;      /* Property list pointer */
    H5VL_rados_file_t *file = NULL;
    void *ret_value = NULL;

    FUNC_ENTER_NOAPI_NOINIT

    /*
     * Adjust bit flags by turning on the creation bit and making sure that
     * the EXCL or TRUNC bit is set.  All newly-created files are opened for
     * reading and writing.
     */
    if(0 == (flags & (H5F_ACC_EXCL|H5F_ACC_TRUNC)))
        flags |= H5F_ACC_EXCL;      /*default*/
    flags |= H5F_ACC_RDWR | H5F_ACC_CREAT;

    /* Get information from the FAPL */
    if(NULL == (plist = H5P_object_verify(fapl_id, H5P_FILE_ACCESS)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, NULL, "not a file access property list")
    if(H5P_get_vol_info(plist, (void **)&info) < 0)
        HGOTO_ERROR(H5E_FILE, H5E_CANTGET, NULL, "can't get RADOS info struct")

    /* allocate the file object that is returned to the user */
    if(NULL == (file = H5FL_CALLOC(H5VL_rados_file_t)))
        HGOTO_ERROR(H5E_FILE, H5E_CANTALLOC, NULL, "can't allocate RADOS file struct")
    file->glob_md_oid = NULL;
    file->root_grp = NULL;
    file->fcpl_id = FAIL;
    file->fapl_id = FAIL;

    /* Fill in fields of file we know */
    file->item.type = H5I_FILE;
    file->item.file = file;
    file->item.rc = 1;
    if(NULL == (file->file_name = HDstrdup(name)))
        HGOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, NULL, "can't copy file name")
    file->file_name_len = HDstrlen(name);
    file->flags = flags;
    file->max_oid = 0;
    if(H5VL_rados_oid_create_string(file, file->max_oid, &file->glob_md_oid) < 0)
        HGOTO_ERROR(H5E_FILE, H5E_CANTINIT, NULL, "can't create oid for globabl metadata object")
    file->max_oid_dirty = FALSE;
    if((file->fcpl_id = H5Pcopy(fcpl_id)) < 0)
        HGOTO_ERROR(H5E_FILE, H5E_CANTCOPY, NULL, "failed to copy fcpl")
    if((file->fapl_id = H5Pcopy(fapl_id)) < 0)
        HGOTO_ERROR(H5E_FILE, H5E_CANTCOPY, NULL, "failed to copy fapl")

    /* Duplicate communicator and Info object. */
    if(FAIL == H5FD_mpi_comm_info_dup(info->comm, info->info, &file->comm, &file->info))
        HGOTO_ERROR(H5E_INTERNAL, H5E_CANTCOPY, NULL, "Communicator/Info duplicate failed")

    /* Obtain the process rank and size from the communicator attached to the
     * fapl ID */
    MPI_Comm_rank(info->comm, &file->my_rank);
    MPI_Comm_size(info->comm, &file->num_procs);

    /* Determine if we requested collective object ops for the file */
    if(H5Pget_all_coll_metadata_ops(fapl_id, &file->collective) < 0)
        HGOTO_ERROR(H5E_FILE, H5E_CANTGET, NULL, "can't get collective access property")
 
    /* Create root group */
    if(NULL == (file->root_grp = (H5VL_rados_group_t *)H5VL_rados_group_create_helper(file, fcpl_id, H5P_GROUP_ACCESS_DEFAULT, dxpl_id, req, NULL, NULL, TRUE)))
        HGOTO_ERROR(H5E_FILE, H5E_CANTINIT, NULL, "can't create root group")

    /* Create root group oid */
    HDassert(H5VL_rados_oid_to_idx(file->root_grp->obj.bin_oid) == (uint64_t)1);

    /* Free info */
    if(H5VL_rados_info_free(info) < 0)
        HGOTO_ERROR(H5E_VOL, H5E_CANTFREE, NULL, "can't free connector info")

    ret_value = (void *)file;

done:
    /* Cleanup on failure */
    if(NULL == ret_value) {
        /* Free info */
        if(info)
            if(H5VL_rados_info_free(info) < 0)
                HGOTO_ERROR(H5E_VOL, H5E_CANTFREE, NULL, "can't free connector info")

        /* Close file */
        if(file && H5VL_rados_file_close_helper(file, dxpl_id, req) < 0)
            HDONE_ERROR(H5E_FILE, H5E_CANTCLOSEFILE, NULL, "can't close file")
    } /* end if */

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5VL_rados_file_create() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_rados_file_open
 *
 * Purpose:     Opens a file as a RADOS HDF5 file.
 *
 * Return:      Success:        the file id. 
 *              Failure:        NULL
 *
 * Programmer:  Neil Fortner
 *              February, 2018
 *
 *-------------------------------------------------------------------------
 */
static void *
H5VL_rados_file_open(const char *name, unsigned flags, hid_t fapl_id,
    hid_t dxpl_id, void **req)
{
    H5VL_rados_info_t *info = NULL;
    H5P_genplist_t *plist = NULL;      /* Property list pointer */
    H5VL_rados_file_t *file = NULL;
    char foi_buf_static[H5VL_RADOS_FOI_BUF_SIZE];
    char *foi_buf_dyn = NULL;
    char *foi_buf = foi_buf_static;
    void *gcpl_buf = NULL;
    uint64_t gcpl_len;
    uint64_t root_grp_oid;
    hbool_t must_bcast = FALSE;
    uint8_t *p;
    int ret;
    void *ret_value = NULL;

    FUNC_ENTER_NOAPI_NOINIT

    /* Get information from the FAPL */
    if(NULL == (plist = H5P_object_verify(fapl_id, H5P_FILE_ACCESS)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, NULL, "not a file access property list")
    if(H5P_get_vol_info(plist, (void **)&info) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTGET, NULL, "can't get RADOS info struct")

    /* allocate the file object that is returned to the user */
    if(NULL == (file = H5FL_CALLOC(H5VL_rados_file_t)))
        HGOTO_ERROR(H5E_FILE, H5E_CANTALLOC, NULL, "can't allocate RADOS file struct")
    //file->glob_md_oh = DAOS_HDL_INVAL;
    file->root_grp = NULL;
    file->fcpl_id = FAIL;
    file->fapl_id = FAIL;

    /* Fill in fields of file we know */
    file->item.type = H5I_FILE;
    file->item.file = file;
    file->item.rc = 1;
    if(NULL == (file->file_name = HDstrdup(name)))
        HGOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, NULL, "can't copy file name")
    file->file_name_len = HDstrlen(name);
    file->flags = flags;
    if(H5VL_rados_oid_create_string(file, file->max_oid, &file->glob_md_oid) < 0)
        HGOTO_ERROR(H5E_FILE, H5E_CANTINIT, NULL, "can't create oid for globabl metadata object")
    if((file->fapl_id = H5Pcopy(fapl_id)) < 0)
        HGOTO_ERROR(H5E_FILE, H5E_CANTCOPY, NULL, "failed to copy fapl")

    /* Duplicate communicator and Info object. */
    if(FAIL == H5FD_mpi_comm_info_dup(info->comm, info->info, &file->comm, &file->info))
        HGOTO_ERROR(H5E_INTERNAL, H5E_CANTCOPY, NULL, "Communicator/Info duplicate failed")

    /* Obtain the process rank and size from the communicator attached to the
     * fapl ID */
    MPI_Comm_rank(info->comm, &file->my_rank);
    MPI_Comm_size(info->comm, &file->num_procs);

    /* Generate root group oid */
    H5VL_rados_oid_create_binary((uint64_t)1, H5I_GROUP, &root_grp_oid);

    /* Determine if we requested collective object ops for the file */
    if(H5Pget_all_coll_metadata_ops(fapl_id, &file->collective) < 0)
        HGOTO_ERROR(H5E_FILE, H5E_CANTGET, NULL, "can't get collective access property")

    if(file->my_rank == 0) {
        /* If there are other processes and we fail we must bcast anyways so they
         * don't hang */
        if(file->num_procs > 1)
            must_bcast = TRUE;

        /* Read max oid directly to foi_buf */
        /* Check for does not exist here and assume 0? -NAF */
        if((ret = H5VL_rados_read(ioctx_g, file->glob_md_oid, foi_buf, 8, 0)) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTDECODE, NULL, "can't read metadata from dataset: %s", strerror(-ret))

        /* Decode max oid */
        p = (uint8_t *)foi_buf;
        UINT64DECODE(p, file->max_oid)

        /* Open root group */
        if(NULL == (file->root_grp = (H5VL_rados_group_t *)H5VL_rados_group_open_helper(file, root_grp_oid, H5P_GROUP_ACCESS_DEFAULT, dxpl_id, req, (file->num_procs > 1) ? &gcpl_buf : NULL, &gcpl_len)))
            HGOTO_ERROR(H5E_FILE, H5E_CANTINIT, NULL, "can't open root group")

        /* Bcast global handles if there are other processes */
        if(file->num_procs > 1) {
            /* Check if the file open info won't fit into the static buffer */
            if(gcpl_len + 2 * sizeof(uint64_t) > sizeof(foi_buf_static)) {
                /* Allocate dynamic buffer */
                if(NULL == (foi_buf_dyn = (char *)H5MM_malloc(gcpl_len + 2 * sizeof(uint64_t))))
                    HGOTO_ERROR(H5E_FILE, H5E_CANTALLOC, NULL, "can't allocate space for global container handle")

                /* Use dynamic buffer */
                foi_buf = foi_buf_dyn;

                /* Copy max oid from static buffer */
                HDmemcpy(foi_buf, foi_buf_static, sizeof(uint64_t));
            } /* end if */

            /* Max oid already encoded (read in encoded form from rados) */
            HDassert(p == ((uint8_t *)foi_buf) + sizeof(uint64_t));

            /* Encode GCPL length */
            UINT64ENCODE(p, gcpl_len)

            /* Copy GCPL buffer */
            HDmemcpy(p, gcpl_buf, gcpl_len);

            /* We are about to bcast so we no longer need to bcast on failure */
            must_bcast = FALSE;

            /* MPI_Bcast foi_buf */
            if(MPI_SUCCESS != MPI_Bcast(foi_buf, (int)sizeof(foi_buf_static), MPI_BYTE, 0, info->comm))
                HGOTO_ERROR(H5E_FILE, H5E_MPI, NULL, "can't bcast global container handle")

            /* Need a second bcast if we had to allocate a dynamic buffer */
            if(foi_buf == foi_buf_dyn)
                if(MPI_SUCCESS != MPI_Bcast((char *)p, (int)(gcpl_len), MPI_BYTE, 0, info->comm))
                    HGOTO_ERROR(H5E_FILE, H5E_MPI, NULL, "can't bcast file open info (second bcast)")
        } /* end if */
    } /* end if */
    else {
        HDassert(sizeof(foi_buf_static) >= 2 * sizeof(uint64_t));

        /* Receive file open info */
        if(MPI_SUCCESS != MPI_Bcast(foi_buf, (int)sizeof(foi_buf_static), MPI_BYTE, 0, info->comm))
            HGOTO_ERROR(H5E_FILE, H5E_MPI, NULL, "can't bcast global container handle")

        /* Decode max OID */
        p = (uint8_t *)foi_buf;
        UINT64DECODE(p, file->max_oid)

        /* Decode GCPL length */
        UINT64DECODE(p, gcpl_len)

        /* Check for gcpl_len set to 0 - indicates failure */
        if(gcpl_len == 0)
            HGOTO_ERROR(H5E_FILE, H5E_CANTINIT, NULL, "lead process failed to open file")

        /* Check if we need to perform another bcast */
        if(gcpl_len + 2 * sizeof(uint64_t) > sizeof(foi_buf_static)) {
            /* Check if we need to allocate a dynamic buffer */
            if(gcpl_len > sizeof(foi_buf_static)) {
                /* Allocate dynamic buffer */
                if(NULL == (foi_buf_dyn = (char *)H5MM_malloc(gcpl_len)))
                    HGOTO_ERROR(H5E_FILE, H5E_CANTALLOC, NULL, "can't allocate space for global pool handle")
                foi_buf = foi_buf_dyn;
            } /* end if */

            /* Receive info buffer */
            if(MPI_SUCCESS != MPI_Bcast(foi_buf_dyn, (int)(gcpl_len), MPI_BYTE, 0, info->comm))
                HGOTO_ERROR(H5E_FILE, H5E_MPI, NULL, "can't bcast global container handle (second bcast)")

            p = (uint8_t *)foi_buf;
        } /* end if */

        /* Reconstitute root group from revieved GCPL */
        if(NULL == (file->root_grp = (H5VL_rados_group_t *)H5VL_rados_group_reconstitute(file, root_grp_oid, p, H5P_GROUP_ACCESS_DEFAULT, dxpl_id, req)))
            HGOTO_ERROR(H5E_FILE, H5E_CANTINIT, NULL, "can't reconstitute root group")
    } /* end else */

    /* FCPL was stored as root group's GCPL (as GCPL is the parent of FCPL).
     * Point to it. */
    file->fcpl_id = file->root_grp->gcpl_id;
    if(H5Iinc_ref(file->fcpl_id) < 0)
        HGOTO_ERROR(H5E_ATOM, H5E_CANTINC, NULL, "can't increment FCPL ref count")

    /* Free info */
    if(H5VL_rados_info_free(info) < 0)
        HGOTO_ERROR(H5E_VOL, H5E_CANTFREE, NULL, "can't free connector info")

    ret_value = (void *)file;

done:
    /* Cleanup on failure */
    if(NULL == ret_value) {
        /* Free info */
        if(info)
            if(H5VL_rados_info_free(info) < 0)
                HGOTO_ERROR(H5E_VOL, H5E_CANTFREE, NULL, "can't free connector info")

        /* Bcast bcast_buf_64 as '0' if necessary - this will trigger failures
         * in the other processes so we do not need to do the second bcast. */
        if(must_bcast) {
            HDmemset(foi_buf_static, 0, sizeof(foi_buf_static));
            if(MPI_SUCCESS != MPI_Bcast(foi_buf_static, sizeof(foi_buf_static), MPI_BYTE, 0, info->comm))
                HDONE_ERROR(H5E_FILE, H5E_MPI, NULL, "can't bcast global handle sizes")
        } /* end if */

        /* Close file */
        if(file && H5VL_rados_file_close_helper(file, dxpl_id, req) < 0)
            HDONE_ERROR(H5E_FILE, H5E_CANTCLOSEFILE, NULL, "can't close file")
    } /* end if */

    /* Clean up buffers */
    foi_buf_dyn = (char *)H5MM_xfree(foi_buf_dyn);
    gcpl_buf = H5MM_xfree(gcpl_buf);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5VL_rados_file_open() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_rados_file_flush
 *
 * Purpose:     Flushes a RADOS file.  Currently just writes the max oid.
 *
 * Return:      Success:        0
 *              Failure:        -1
 *
 * Programmer:  Neil Fortner
 *              April, 2018
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_rados_file_flush(H5VL_rados_file_t *file)
{
    herr_t       ret_value = SUCCEED;    /* Return value */

    FUNC_ENTER_NOAPI_NOINIT

    /* Nothing to do if no write intent */
    if(!(file->flags & H5F_ACC_RDWR))
        HGOTO_DONE(SUCCEED)

    /* Write max oid */
    if(H5VL_rados_write_max_oid(file) < 0)
        HGOTO_ERROR(H5E_FILE, H5E_CANTINIT, FAIL, "can't write max OID")

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5VL_rados_file_flush() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_rados_file_specific
 *
 * Purpose:     Perform an operation
 *
 * Return:      Success:        0
 *              Failure:        -1
 *
 * Programmer:  Neil Fortner
 *              April, 2018
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_rados_file_specific(void *item, H5VL_file_specific_t specific_type,
    hid_t H5_ATTR_UNUSED dxpl_id, void H5_ATTR_UNUSED **req,
    va_list H5_ATTR_UNUSED arguments)
{
    herr_t       ret_value = SUCCEED;    /* Return value */

    FUNC_ENTER_NOAPI_NOINIT

    switch (specific_type) {
        /* H5Fflush` */
        case H5VL_FILE_FLUSH:
        {
            H5VL_rados_file_t *file = ((H5VL_rados_item_t *)item)->file;

            if(H5VL_rados_file_flush(file) < 0)
                HGOTO_ERROR(H5E_FILE, H5E_WRITEERROR, FAIL, "can't flush file")

            break;
        }
        /* H5Fmount */
        case H5VL_FILE_MOUNT:
        /* H5Fmount */
        case H5VL_FILE_UNMOUNT:
        /* H5Fis_accessible */
        case H5VL_FILE_IS_ACCESSIBLE:
        {
            hid_t       fapl_id = HDva_arg(arguments, hid_t);
            const char *name    = HDva_arg(arguments, const char *);
            htri_t     *ret     = HDva_arg(arguments, htri_t *);
            char       *glob_md_oid = NULL;
            uint64_t    gcpl_len = 0;
            time_t      pmtime;

            /* Get global metadata ID */
            if(H5VL_rados_oid_create_string_name(name, HDstrlen(name), 0, &glob_md_oid) < 0)
                HGOTO_ERROR(H5E_FILE, H5E_CANTENCODE, FAIL, "can't encode string oid")

            /* Get the object size and time */
            if(H5VL_rados_stat(ioctx_g, glob_md_oid, &gcpl_len, &pmtime) < 0)
                HGOTO_ERROR(H5E_FILE, H5E_CANTGET, FAIL, "can't read metadata size from group")

            /* Check for metadata not found */
            if(gcpl_len == (uint64_t)0)
                *ret = FALSE;
            else
                *ret = TRUE;
            break;
        }

        case H5VL_FILE_REOPEN:
        default:
            HGOTO_ERROR(H5E_VOL, H5E_UNSUPPORTED, FAIL, "invalid or unsupported specific operation")
    } /* end switch */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5VL_rados_file_specific() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_rados_file_close_helper
 *
 * Purpose:     Closes a RADOS HDF5 file.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 * Programmer:  Neil Fortner
 *              February, 2018
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_rados_file_close_helper(H5VL_rados_file_t *file, hid_t dxpl_id, void **req)
{
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_NOAPI_NOINIT

    HDassert(file);

    /* Free file data structures */
    if(file->file_name)
        HDfree(file->file_name);
    file->glob_md_oid = (char *)(char *)(char *)(char *)(char *)(char *)(char *)(char *)(char *)H5MM_xfree(file->glob_md_oid);
    if(file->comm || file->info)
        if(H5FD_mpi_comm_info_free(&file->comm, &file->info) < 0)
            HDONE_ERROR(H5E_INTERNAL, H5E_CANTFREE, FAIL, "Communicator/Info free failed")
    /* Note: Use of H5I_dec_app_ref is a hack, using H5I_dec_ref doesn't reduce
     * app reference count incremented by use of public API to create the ID,
     * while use of H5Idec_ref clears the error stack.  In general we can't use
     * public APIs in the "done" section or in close routines for this reason,
     * until we implement a separate error stack for the VOL plugin */
    if(file->fapl_id != FAIL && H5I_dec_app_ref(file->fapl_id) < 0)
        HDONE_ERROR(H5E_SYM, H5E_CANTDEC, FAIL, "failed to close plist")
    if(file->fcpl_id != FAIL && H5I_dec_app_ref(file->fcpl_id) < 0)
        HDONE_ERROR(H5E_SYM, H5E_CANTDEC, FAIL, "failed to close plist")
    if(file->root_grp)
        if(H5VL_rados_group_close(file->root_grp, dxpl_id, req) < 0)
            HDONE_ERROR(H5E_FILE, H5E_CANTCLOSEFILE, FAIL, "can't close root group")
    file = H5FL_FREE(H5VL_rados_file_t, file);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5VL_rados_file_close_helper() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_rados_file_close
 *
 * Purpose:     Closes a RADOS  HDF5 file.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 * Programmer:  Neil Fortner
 *              February, 2018
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_rados_file_close(void *_file, hid_t dxpl_id, void **req)
{
    H5VL_rados_file_t *file = (H5VL_rados_file_t *)_file;
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_NOAPI_NOINIT

    HDassert(file);

    /* Flush the file */
    if(H5VL_rados_file_flush(file) < 0)
        HGOTO_ERROR(H5E_FILE, H5E_WRITEERROR, FAIL, "can't flush file")

    /* Close the file */
    if(H5VL_rados_file_close_helper(file, dxpl_id, req) < 0)
        HGOTO_ERROR(H5E_FILE, H5E_CANTCLOSEFILE, FAIL, "can't close file")

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5VL_rados_file_close() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_rados_write_max_oid
 *
 * Purpose:     Writes the max OID (object index) to the global metadata
 *              object
 *
 * Return:      Success:        0
 *              Failure:        1
 *
 * Programmer:  Neil Fortner
 *              April, 2018
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_rados_write_max_oid(H5VL_rados_file_t *file)
{
    int ret;
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_NOAPI_NOINIT

    /* Write max oid to global metadata object if necessary */
    if(file->max_oid_dirty) {
        uint8_t wbuf[8];
        uint8_t *p = wbuf;

        UINT64ENCODE(p, file->max_oid)

        if((ret = H5VL_rados_write_full(ioctx_g, file->glob_md_oid, (const char *)wbuf, (size_t)8)) < 0)
            HGOTO_ERROR(H5E_SYM, H5E_CANTINIT, FAIL, "can't write metadata to group: %s", strerror(-ret))
        file->max_oid_dirty = FALSE;
    } /* end if */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5VL_rados_write_max_oid() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_rados_link_read
 *
 * Purpose:     Reads the specified link from the given group.  Note that
 *              if the returned link is a soft link, val->target.soft must
 *              eventually be freed.
 *
 * Return:      Success:        SUCCEED 
 *              Failure:        FAIL
 *
 * Programmer:  Neil Fortner
 *              December, 2016
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_rados_link_read(H5VL_rados_group_t *grp, const char *name,
    H5VL_rados_link_val_t *val)
{
    rados_read_op_t read_op;
    hbool_t read_op_init = FALSE;
    rados_omap_iter_t iter;
    hbool_t iter_init = FALSE;
    char *key;
    char *omap_val;
    size_t val_len;
    uint8_t *p;
    int ret;
    int read_ret;
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_NOAPI_NOINIT

    /* Create read op */
    if(NULL == (read_op = rados_create_read_op()))
        HGOTO_ERROR(H5E_SYM, H5E_CANTINIT, FAIL, "can't create read operation")
    read_op_init = TRUE;

    /* Add operation to get link value */
    /* Add prefix RADOSINC */
    rados_read_op_omap_get_vals_by_keys(read_op, (const char * const *)&name, 1, &iter, &read_ret);
    iter_init = TRUE;

    /* Execute read operation */
    if((ret = rados_read_op_operate(read_op, ioctx_g, grp->obj.oid, LIBRADOS_OPERATION_NOFLAG)) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTINIT, FAIL, "can't perform read operation: %s", strerror(-ret))
    if(read_ret < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTINIT, FAIL, "can't read link value: %s", strerror(-read_ret))

    /* Get link value */
    if((ret = rados_omap_get_next(iter, &key, &omap_val, &val_len)) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTINIT, FAIL, "can't get link value: %s", strerror(-ret))

    /* Check for no link found */
    if(val_len == 0)
        HGOTO_ERROR(H5E_SYM, H5E_NOTFOUND, FAIL, "link not found")

    /* Decode link type */
    p = (uint8_t *)omap_val;
    val->type = (H5L_type_t)*p++;

    /* Decode remainder of link value */
    switch(val->type) {
        case H5L_TYPE_HARD:
            /* Decode oid */
            UINT64DECODE(p, val->target.hard)

            break;

        case H5L_TYPE_SOFT:
            /* Allocate soft link buffer and copy string. */
            if(NULL == (val->target.soft = (char *)H5MM_malloc(val_len)))
                HGOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't allocate link value buffer")
            HDmemcpy(val->target.soft, val + 1, val_len - 1);

            /* Add null terminator */
            val->target.soft[val_len - 1] = '\0';

            break;

        case H5L_TYPE_ERROR:
        case H5L_TYPE_EXTERNAL:
        case H5L_TYPE_MAX:
        default:
            HGOTO_ERROR(H5E_SYM, H5E_BADVALUE, FAIL, "invalid or unsupported link type")
    } /* end switch */

done:
    if(iter_init)
        rados_omap_get_end(iter);
    if(read_op_init)
        rados_release_read_op(read_op);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5VL_rados_link_read() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_rados_link_write
 *
 * Purpose:     Writes the specified link to the given group
 *
 * Return:      Success:        SUCCEED 
 *              Failure:        FAIL
 *
 * Programmer:  Neil Fortner
 *              February, 2018
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_rados_link_write(H5VL_rados_group_t *grp, const char *name,
    H5VL_rados_link_val_t *val)
{
    rados_write_op_t write_op;
    hbool_t write_op_init = FALSE;
    size_t val_len;
    uint8_t *val_buf;
    uint8_t val_buf_static[H5VL_RADOS_LINK_VAL_BUF_SIZE];
    uint8_t *val_buf_dyn = NULL;
    uint8_t *p;
    int ret;
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_NOAPI_NOINIT

    /* Check for write access */
    if(!(grp->obj.item.file->flags & H5F_ACC_RDWR))
        HGOTO_ERROR(H5E_FILE, H5E_BADVALUE, FAIL, "no write intent on file")

    val_buf = val_buf_static;

    /* Encode type specific value information */
    switch(val->type) {
         case H5L_TYPE_HARD:
            HDassert(sizeof(val_buf_static) >= sizeof(val->target.hard) + 1);

            /* Encode link type */
            p = val_buf;
            *p++ = (uint8_t)val->type;

            /* Encode oid */
            UINT64ENCODE(p, val->target.hard)

            val_len = (size_t)9;

            break;

        case H5L_TYPE_SOFT:
            /* Allocate larger buffer for soft link if necessary */
            val_len = HDstrlen(val->target.soft) + 1;
            if(val_len > sizeof(val_buf_static)) {
                if(NULL == (val_buf_dyn = (uint8_t *)H5MM_malloc(val_len)))
                    HGOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't allocate link value buffer")
                val_buf = val_buf_dyn;
            } /* end if */

            /* Encode link type */
            p = val_buf;
            *p++ = (uint8_t)val->type;

            /* Copy link target */
            HDmemcpy(p, val->target.soft, val_len - 1);

            break;

        case H5L_TYPE_ERROR:
        case H5L_TYPE_EXTERNAL:
        case H5L_TYPE_MAX:
        default:
            HGOTO_ERROR(H5E_SYM, H5E_BADVALUE, FAIL, "invalid or unsupported link type")
    } /* end switch */

    /* Create write op */
    if(NULL == (write_op = rados_create_write_op()))
        HGOTO_ERROR(H5E_SYM, H5E_CANTINIT, FAIL, "can't create write operation")
    write_op_init = TRUE;

    /* Add operation to write link */
    /* Add prefix RADOSINC */
    rados_write_op_omap_set(write_op, &name, (const char * const *)&val_buf, &val_len, 1);

    /* Execute write operation */
    if((ret = rados_write_op_operate(write_op, ioctx_g, grp->obj.oid, NULL, LIBRADOS_OPERATION_NOFLAG)) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTINIT, FAIL, "can't perform write operation: %s", strerror(-ret))

done:
    if(write_op_init)
        rados_release_write_op(write_op);
    H5MM_xfree(val_buf_dyn);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5VL_rados_link_write() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_rados_link_follow
 *
 * Purpose:     Follows the link in grp identified with name, and returns
 *              in oid the oid of the target object.  name must be NULL
 *              terminated.
 *
 * Return:      Success:        SUCCEED 
 *              Failure:        FAIL
 *
 * Programmer:  Neil Fortner
 *              February, 2018
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_rados_link_follow(H5VL_rados_group_t *grp, const char *name, hid_t dxpl_id,
    void **req, uint64_t *oid)
{
    H5VL_rados_link_val_t link_val;
    hbool_t link_val_alloc = FALSE;
    H5VL_rados_group_t *target_grp = NULL;
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_NOAPI_NOINIT

    HDassert(grp);
    HDassert(name);
    HDassert(oid);

    /* Read link to group */
   if(H5VL_rados_link_read(grp, name, &link_val) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTINIT, FAIL, "can't read link")

    switch(link_val.type) {
       case H5L_TYPE_HARD:
            /* Simply return the read oid */
            *oid = link_val.target.hard;

            break;

        case H5L_TYPE_SOFT:
            {
                char *target_name = NULL;

                link_val_alloc = TRUE;

                /* Traverse the soft link path */
                if(NULL == (target_grp = H5VL_rados_group_traverse(&grp->obj.item, link_val.target.soft, dxpl_id, req, &target_name, NULL, NULL)))
                    HGOTO_ERROR(H5E_SYM, H5E_BADITER, FAIL, "can't traverse path")

                /* Check for no target_name, in this case just return
                 * target_grp's oid */
                if(target_name[0] == '\0'
                        || (target_name[0] == '.' && target_name[1] == '\0'))
                    *oid = target_grp->obj.bin_oid;
                else
                    /* Follow the last element in the path */
                    if(H5VL_rados_link_follow(target_grp, target_name, dxpl_id, req, oid) < 0)
                        HGOTO_ERROR(H5E_SYM, H5E_CANTINIT, FAIL, "can't follow link")

                break;
            } /* end block */

        case H5L_TYPE_ERROR:
        case H5L_TYPE_EXTERNAL:
        case H5L_TYPE_MAX:
        default:
           HGOTO_ERROR(H5E_SYM, H5E_BADVALUE, FAIL, "invalid or unsupported link type")
    } /* end switch */

done:
    /* Clean up */
    if(link_val_alloc) {
        HDassert(link_val.type == H5L_TYPE_SOFT);
        H5MM_free(link_val.target.soft);
    } /* end if */

    if(target_grp)
        if(H5VL_rados_group_close(target_grp, dxpl_id, req) < 0)
            HGOTO_ERROR(H5E_SYM, H5E_CLOSEERROR, FAIL, "can't close group")

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5VL_rados_link_follow() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_rados_link_follow_comp
 *
 * Purpose:     Follows the link in grp identified with name, and returns
 *              in oid the oid of the target object.  name may be a
 *              component of a path, only the first name_len bytes of name
 *              are examined.
 *
 * Return:      Success:        SUCCEED 
 *              Failure:        FAIL
 *
 * Programmer:  Neil Fortner
 *              April, 2018
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_rados_link_follow_comp(H5VL_rados_group_t *grp, char *name,
    size_t name_len, hid_t dxpl_id, void **req, uint64_t *oid)
{
    char saved_end = name[name_len];
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_NOAPI_NOINIT

    HDassert(grp);
    HDassert(name);
    HDassert(oid);

    /* Add null terminator to name so we can use the underlying routine */
    name[name_len] = '\0';

    /* Follow the link now that name is NULL terminated */
    if(H5VL_rados_link_follow(grp, name, dxpl_id, req, oid) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTINIT, FAIL, "can't follow link to group")

done:
    /* Put name back the way it was */
    name[name_len] = saved_end;

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5VL_rados_link_follow_comp() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_rados_group_traverse
 *
 * Purpose:     Given a path name and base object, returns the final group
 *              in the path and the object name.  obj_name points into the
 *              buffer given by path, so it does not need to be freed.
 *              The group must be closed with H5VL_rados_group_close().
 *
 * Return:      Success:        group object. 
 *              Failure:        NULL
 *
 * Programmer:  Neil Fortner
 *              February, 2018
 *
 *-------------------------------------------------------------------------
 */
static H5VL_rados_group_t *
H5VL_rados_group_traverse(H5VL_rados_item_t *item, char *path,
    hid_t dxpl_id, void **req, char **obj_name, void **gcpl_buf_out,
    uint64_t *gcpl_len_out)
{
    H5VL_rados_group_t *grp = NULL;
    char *next_obj;
    uint64_t oid;
    H5VL_rados_group_t *ret_value = NULL;

    FUNC_ENTER_NOAPI_NOINIT

    HDassert(item);
    HDassert(path);
    HDassert(obj_name);

    /* Initialize obj_name */
    *obj_name = path;

    /* Open starting group */
    if((*obj_name)[0] == '/') {
        grp = item->file->root_grp;
        (*obj_name)++;
    } /* end if */
    else {
        if(item->type == H5I_GROUP)
            grp = (H5VL_rados_group_t *)item;
        else if(item->type == H5I_FILE)
            grp = ((H5VL_rados_file_t *)item)->root_grp;
        else
            HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, NULL, "item not a file or group")
    } /* end else */
        
    grp->obj.item.rc++;

    /* Search for '/' */
    next_obj = strchr(*obj_name, '/');

    /* Traverse path */
    while(next_obj) {
        /* Free gcpl_buf_out */
        if(gcpl_buf_out)
            *gcpl_buf_out = H5MM_xfree(*gcpl_buf_out);

        /* Follow link to next group in path */
        HDassert(next_obj > *obj_name);
        if(H5VL_rados_link_follow_comp(grp, *obj_name, (size_t)(next_obj - *obj_name), dxpl_id, req, &oid) < 0)
            HGOTO_ERROR(H5E_SYM, H5E_CANTINIT, NULL, "can't follow link to group")

        /* Close previous group */
        if(H5VL_rados_group_close(grp, dxpl_id, req) < 0)
            HGOTO_ERROR(H5E_SYM, H5E_CLOSEERROR, NULL, "can't close group")
        grp = NULL;

        /* Open group */
        if(NULL == (grp = (H5VL_rados_group_t *)H5VL_rados_group_open_helper(item->file, oid, H5P_GROUP_ACCESS_DEFAULT, dxpl_id, req, gcpl_buf_out, gcpl_len_out)))
            HGOTO_ERROR(H5E_SYM, H5E_CANTINIT, NULL, "can't open group")

        /* Advance to next path element */
        *obj_name = next_obj + 1;
        next_obj = strchr(*obj_name, '/');
    } /* end while */

    /* Set return values */
    ret_value = grp;

done:
    /* Cleanup on failure */
    if(NULL == ret_value)
        /* Close group */
        if(grp && H5VL_rados_group_close(grp, dxpl_id, req) < 0)
            HDONE_ERROR(H5E_FILE, H5E_CLOSEERROR, NULL, "can't close group")

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5VL_rados_group_traverse() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_rados_group_traverse_const
 *
 * Purpose:     Wrapper for H5VL_rados_group_traverse for a const path.
 *
 * Return:      Success:        group object. 
 *              Failure:        NULL
 *
 * Programmer:  Neil Fortner
 *              April, 2018
 *
 *-------------------------------------------------------------------------
 */
static H5VL_rados_group_t *
H5VL_rados_group_traverse_const(H5VL_rados_item_t *item, const char *path,
    hid_t dxpl_id, void **req, const char **obj_name, void **gcpl_buf_out,
    uint64_t *gcpl_len_out)
{
    H5VL_rados_group_t *grp = NULL;
    char *tmp_path = NULL;
    char *tmp_obj_name;
    H5VL_rados_group_t *ret_value = NULL;

    FUNC_ENTER_NOAPI_NOINIT

    HDassert(item);
    HDassert(path);
    HDassert(obj_name);

    /* Make a temporary copy of path so we do not write to the user's const
     * buffer (since the RADOS API expects null terminated strings we must
     * insert null terminators to pass path components to RADOS.  We could
     * alternatively copy each path name but this is simpler and shares more
     * code with other VOL plugins) */
    if(NULL == (tmp_path = HDstrdup(path)))
        HGOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, NULL, "can't duplicate path name")

    /* Forward the call to the non-const routine */
    if(NULL == (grp = H5VL_rados_group_traverse(item, tmp_path, dxpl_id, req,
            &tmp_obj_name, gcpl_buf_out, gcpl_len_out)))
        HGOTO_ERROR(H5E_SYM, H5E_BADITER, NULL, "can't traverse path")

    /* Set *obj_name in path to match tmp_obj_name in tmp_path */
    *obj_name = path + (tmp_obj_name - tmp_path);

    /* Set return value */
    ret_value = grp;

done:
    /* Cleanup on failure */
    if(NULL == ret_value)
        /* Close group */
        if(grp && H5VL_rados_group_close(grp, dxpl_id, req) < 0)
            HDONE_ERROR(H5E_FILE, H5E_CLOSEERROR, NULL, "can't close group")

    H5MM_xfree(tmp_path);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5VL_rados_group_traverse_const() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_rados_group_create_helper
 *
 * Purpose:     Performs the actual group creation.
 *
 * Return:      Success:        group object. 
 *              Failure:        NULL
 *
 * Programmer:  Neil Fortner
 *              January, 2018
 *
 *-------------------------------------------------------------------------
 */
static void *
H5VL_rados_group_create_helper(H5VL_rados_file_t *file, hid_t gcpl_id,
    hid_t gapl_id, hid_t dxpl_id, void **req, H5VL_rados_group_t *parent_grp,
    const char *name, hbool_t collective)
{
    H5VL_rados_group_t *grp = NULL;
    void *gcpl_buf = NULL;
    int ret;
    void *ret_value = NULL;

    FUNC_ENTER_NOAPI_NOINIT

    HDassert(file->flags & H5F_ACC_RDWR);

    /* Allocate the group object that is returned to the user */
    if(NULL == (grp = H5FL_CALLOC(H5VL_rados_group_t)))
        HGOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, NULL, "can't allocate RADOS group struct")
    grp->obj.item.type = H5I_GROUP;
    grp->obj.item.file = file;
    grp->obj.item.rc = 1;
    grp->gcpl_id = FAIL;
    grp->gapl_id = FAIL;

    /* Generate group oid */
    if(H5VL_rados_oid_create(file, file->max_oid + (uint64_t)1, H5I_GROUP, &grp->obj.bin_oid, &grp->obj.oid) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTINIT, NULL, "can't generate group oid")

    /* Update max_oid */
    file->max_oid = H5VL_rados_oid_to_idx(grp->obj.bin_oid);

    /* Create group and write metadata if this process should */
    if(!collective || (file->my_rank == 0)) {
        size_t gcpl_size = 0;

        /* Create group */
        /* Write max OID */
        /*if(H5VL_rados_write_max_oid(file) < 0)
            HGOTO_ERROR(H5E_FILE, H5E_CANTINIT, NULL, "can't write max OID")*/

        /* Encode GCPL */
        if(H5Pencode(gcpl_id, NULL, &gcpl_size) < 0)
            HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, NULL, "can't determine serialized length of gcpl")
        if(NULL == (gcpl_buf = H5MM_malloc(gcpl_size)))
            HGOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, NULL, "can't allocate buffer for serialized gcpl")
        if(H5Pencode(gcpl_id, gcpl_buf, &gcpl_size) < 0)
            HGOTO_ERROR(H5E_SYM, H5E_CANTENCODE, NULL, "can't serialize gcpl")

        /* Write internal metadata to group */
        if((ret = H5VL_rados_write_full(ioctx_g, grp->obj.oid, (char *)gcpl_buf, gcpl_size)) < 0)
            HGOTO_ERROR(H5E_SYM, H5E_CANTINIT, NULL, "can't write metadata to group: %s", strerror(-ret))

        /* Mark max OID as dirty */
        file->max_oid_dirty = TRUE;

        /* Write link to group if requested */
        if(parent_grp) {
            H5VL_rados_link_val_t link_val;

            HDassert(name);

            link_val.type = H5L_TYPE_HARD;
            link_val.target.hard = grp->obj.bin_oid;
            if(H5VL_rados_link_write(parent_grp, name, &link_val) < 0)
                HGOTO_ERROR(H5E_SYM, H5E_CANTINIT, NULL, "can't create link to group")
        } /* end if */
    } /* end if */

    /* Finish setting up group struct */
    if((grp->gcpl_id = H5Pcopy(gcpl_id)) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTCOPY, NULL, "failed to copy gcpl");
    if((grp->gapl_id = H5Pcopy(gapl_id)) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTCOPY, NULL, "failed to copy gapl");

    ret_value = (void *)grp;

done:
    /* Cleanup on failure */
    if(NULL == ret_value)
        /* Close group */
        if(grp && H5VL_rados_group_close(grp, dxpl_id, req) < 0)
            HDONE_ERROR(H5E_FILE, H5E_CLOSEERROR, NULL, "can't close group")

    /* Free memory */
    gcpl_buf = H5MM_xfree(gcpl_buf);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5VL_rados_group_create_helper() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_rados_group_create
 *
 * Purpose:     Sends a request to RADOS to create a group
 *
 * Return:      Success:        group object. 
 *              Failure:        NULL
 *
 * Programmer:  Neil Fortner
 *              February, 2018
 *
 *-------------------------------------------------------------------------
 */
static void *
H5VL_rados_group_create(void *_item,
    const H5VL_loc_params_t H5_ATTR_UNUSED *loc_params, const char *name,
    hid_t gcpl_id, hid_t gapl_id, hid_t dxpl_id, void **req)
{
    H5VL_rados_item_t *item = (H5VL_rados_item_t *)_item;
    H5VL_rados_group_t *grp = NULL;
    H5VL_rados_group_t *target_grp = NULL;
    const char *target_name = NULL;
    hbool_t collective = item->file->collective;
    void *ret_value = NULL;

    FUNC_ENTER_NOAPI_NOINIT

    /* Check for write access */
    if(!(item->file->flags & H5F_ACC_RDWR))
        HGOTO_ERROR(H5E_FILE, H5E_BADVALUE, NULL, "no write intent on file")
 
    /* Check for collective access, if not already set by the file */
    if(!collective)
        if(H5Pget_all_coll_metadata_ops(gapl_id, &collective) < 0)
            HGOTO_ERROR(H5E_SYM, H5E_CANTGET, NULL, "can't get collective access property")

    /* Traverse the path */
    if(!collective || (item->file->my_rank == 0))
        if(NULL == (target_grp = H5VL_rados_group_traverse_const(item, name, dxpl_id, req, &target_name, NULL, NULL)))
            HGOTO_ERROR(H5E_SYM, H5E_BADITER, NULL, "can't traverse path")

    /* Create group and link to group */
    if(NULL == (grp = (H5VL_rados_group_t *)H5VL_rados_group_create_helper(item->file, gcpl_id, gapl_id, dxpl_id, req, target_grp, target_name, collective)))
        HGOTO_ERROR(H5E_SYM, H5E_CANTINIT, NULL, "can't create group")

    /* Set return value */
    ret_value = (void *)grp;

done:
    /* Close target group */
    if(target_grp && H5VL_rados_group_close(target_grp, dxpl_id, req) < 0)
        HDONE_ERROR(H5E_SYM, H5E_CLOSEERROR, NULL, "can't close group")

    /* Cleanup on failure */
    if(NULL == ret_value)
        /* Close group */
        if(grp && H5VL_rados_group_close(grp, dxpl_id, req) < 0)
            HDONE_ERROR(H5E_SYM, H5E_CLOSEERROR, NULL, "can't close group")

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5VL_rados_group_create() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_rados_group_open_helper
 *
 * Purpose:     Performs the actual group open, given the oid.
 *
 * Return:      Success:        group object. 
 *              Failure:        NULL
 *
 * Programmer:  Neil Fortner
 *              February, 2018
 *
 *-------------------------------------------------------------------------
 */
static void *
H5VL_rados_group_open_helper(H5VL_rados_file_t *file, uint64_t oid,
    hid_t gapl_id, hid_t dxpl_id, void **req, void **gcpl_buf_out,
    uint64_t *gcpl_len_out)
{
    H5VL_rados_group_t *grp = NULL;
    void *gcpl_buf = NULL;
    uint64_t gcpl_len = 0;
    time_t pmtime = 0;
    int ret;
    void *ret_value = NULL;

    FUNC_ENTER_NOAPI_NOINIT

    /* Allocate the group object that is returned to the user */
    if(NULL == (grp = H5FL_CALLOC(H5VL_rados_group_t)))
        HGOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, NULL, "can't allocate RADOS group struct")
    grp->obj.item.type = H5I_GROUP;
    grp->obj.item.file = file;
    grp->obj.item.rc = 1;
    grp->obj.bin_oid = oid;
    if(H5VL_rados_oid_create_string(file, oid, &grp->obj.oid) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTINIT, NULL, "can't encode string oid")
    grp->gcpl_id = FAIL;
    grp->gapl_id = FAIL;

    /* Get the object size and time */
    if((ret = H5VL_rados_stat(ioctx_g, grp->obj.oid, &gcpl_len, &pmtime)) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTDECODE, NULL, "can't read metadata size from group: %s", strerror(-ret))

    /* Check for metadata not found */
    if(gcpl_len == (uint64_t)0)
        HGOTO_ERROR(H5E_SYM, H5E_NOTFOUND, NULL, "internal metadata not found")

    /* Allocate buffer for GCPL */
    if(NULL == (gcpl_buf = H5MM_malloc(gcpl_len)))
        HGOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, NULL, "can't allocate buffer for serialized gcpl")

    /* Read internal metadata from group */
    if((ret = H5VL_rados_read(ioctx_g, grp->obj.oid, (char *)gcpl_buf, gcpl_len, 0)) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTDECODE, NULL, "can't read metadata from group: %s", strerror(-ret))

    /* Decode GCPL */
    if((grp->gcpl_id = H5Pdecode(gcpl_buf)) < 0)
        HGOTO_ERROR(H5E_ARGS, H5E_CANTDECODE, NULL, "can't deserialize GCPL")

    /* Finish setting up group struct */
    if((grp->gapl_id = H5Pcopy(gapl_id)) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTCOPY, NULL, "failed to copy gapl");

    /* Return GCPL info if requested, relinquish ownership of gcpl_buf if so */
    if(gcpl_buf_out) {
        HDassert(gcpl_len_out);
        HDassert(!*gcpl_buf_out);

        *gcpl_buf_out = gcpl_buf;
        gcpl_buf = NULL;

        *gcpl_len_out = gcpl_len;
    } /* end if */

    ret_value = (void *)grp;

done:
    /* Cleanup on failure */
    if(NULL == ret_value)
        /* Close group */
        if(grp && H5VL_rados_group_close(grp, dxpl_id, req) < 0)
            HDONE_ERROR(H5E_SYM, H5E_CLOSEERROR, NULL, "can't close group")

    /* Free memory */
    gcpl_buf = H5MM_xfree(gcpl_buf);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5VL_rados_group_open_helper() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_rados_group_reconstitute
 *
 * Purpose:     Reconstitutes a group object opened by another process.
 *
 * Return:      Success:        group object. 
 *              Failure:        NULL
 *
 * Programmer:  Neil Fortner
 *              February, 2018
 *
 *-------------------------------------------------------------------------
 */
static void *
H5VL_rados_group_reconstitute(H5VL_rados_file_t *file, uint64_t oid,
    uint8_t *gcpl_buf, hid_t gapl_id, hid_t dxpl_id, void **req)
{
    H5VL_rados_group_t *grp = NULL;
    void *ret_value = NULL;

    FUNC_ENTER_NOAPI_NOINIT

    /* Allocate the group object that is returned to the user */
    if(NULL == (grp = H5FL_CALLOC(H5VL_rados_group_t)))
        HGOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, NULL, "can't allocate RADOS group struct")
    grp->obj.item.type = H5I_GROUP;
    grp->obj.item.file = file;
    grp->obj.item.rc = 1;
    grp->obj.bin_oid = oid;
    if(H5VL_rados_oid_create_string(file, oid, &grp->obj.oid) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTINIT, NULL, "can't encode string oid")
    grp->gcpl_id = FAIL;
    grp->gapl_id = FAIL;

    /* Decode GCPL */
    if((grp->gcpl_id = H5Pdecode(gcpl_buf)) < 0)
        HGOTO_ERROR(H5E_ARGS, H5E_CANTDECODE, NULL, "can't deserialize GCPL")

    /* Finish setting up group struct */
    if((grp->gapl_id = H5Pcopy(gapl_id)) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTCOPY, NULL, "failed to copy gapl");

    ret_value = (void *)grp;

done:
    /* Cleanup on failure */
    if(NULL == ret_value)
        /* Close group */
        if(grp && H5VL_rados_group_close(grp, dxpl_id, req) < 0)
            HDONE_ERROR(H5E_SYM, H5E_CLOSEERROR, NULL, "can't close group")

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5VL_rados_group_reconstitute() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_rados_group_open
 *
 * Purpose:     Sends a request to RADOS to open a group
 *
 * Return:      Success:        dataset object. 
 *              Failure:        NULL
 *
 * Programmer:  Neil Fortner
 *              February, 2018
 *
 *-------------------------------------------------------------------------
 */
static void *
H5VL_rados_group_open(void *_item, const H5VL_loc_params_t *loc_params,
    const char *name, hid_t gapl_id, hid_t dxpl_id, void **req)
{
    H5VL_rados_item_t *item = (H5VL_rados_item_t *)_item;
    H5VL_rados_group_t *grp = NULL;
    H5VL_rados_group_t *target_grp = NULL;
    const char *target_name = NULL;
    uint64_t oid;
    uint8_t *gcpl_buf = NULL;
    uint64_t gcpl_len = 0;
    uint8_t ginfo_buf_static[H5VL_RADOS_GINFO_BUF_SIZE];
    uint8_t *p;
    hbool_t collective = item->file->collective;
    hbool_t must_bcast = FALSE;
    void *ret_value = NULL;

    FUNC_ENTER_NOAPI_NOINIT
 
    /* Check for collective access, if not already set by the file */
    if(!collective)
        if(H5Pget_all_coll_metadata_ops(gapl_id, &collective) < 0)
            HGOTO_ERROR(H5E_SYM, H5E_CANTGET, NULL, "can't get collective access property")

    /* Check if we're actually opening the group or just receiving the group
     * info from the leader */
    if(!collective || (item->file->my_rank == 0)) {
        if(collective && (item->file->num_procs > 1))
            must_bcast = TRUE;

        /* Check for open by address */
        if(H5VL_OBJECT_BY_ADDR == loc_params->type) {
            /* Generate oid from address */
            oid = (uint64_t)loc_params->loc_data.loc_by_addr.addr;

            /* Open group */
            if(NULL == (grp = (H5VL_rados_group_t *)H5VL_rados_group_open_helper(item->file, oid, gapl_id, dxpl_id, req, (collective && (item->file->num_procs > 1)) ? (void **)&gcpl_buf : NULL, &gcpl_len)))
                HGOTO_ERROR(H5E_SYM, H5E_CANTOPENOBJ, NULL, "can't open group")
        } /* end if */
        else {
            /* Open using name parameter */
            /* Traverse the path */
            if(NULL == (target_grp = H5VL_rados_group_traverse_const(item, name, dxpl_id, req, &target_name, (collective && (item->file->num_procs > 1)) ? (void **)&gcpl_buf : NULL, &gcpl_len)))
                HGOTO_ERROR(H5E_SYM, H5E_BADITER, NULL, "can't traverse path")

            /* Check for no target_name, in this case just return target_grp */
            if(target_name[0] == '\0'
                    || (target_name[0] == '.' && target_name[1] == '\0')) {
                size_t gcpl_size;

                /* Take ownership of target_grp */
                grp = target_grp;
                target_grp = NULL;

                /* Encode GCPL */
                if(H5Pencode(grp->gcpl_id, NULL, &gcpl_size) < 0)
                    HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, NULL, "can't determine serialized length of gcpl")
                if(NULL == (gcpl_buf = (uint8_t *)H5MM_malloc(gcpl_size)))
                    HGOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, NULL, "can't allocate buffer for serialized gcpl")
                gcpl_len = (uint64_t)gcpl_size;
                if(H5Pencode(grp->gcpl_id, gcpl_buf, &gcpl_size) < 0)
                    HGOTO_ERROR(H5E_SYM, H5E_CANTENCODE, NULL, "can't serialize gcpl")
            } /* end if */
            else {
                gcpl_buf = (uint8_t *)H5MM_xfree(gcpl_buf);
                gcpl_len = 0;

                /* Follow link to group */
                if(H5VL_rados_link_follow(target_grp, target_name, dxpl_id, req, &oid) < 0)
                    HGOTO_ERROR(H5E_SYM, H5E_CANTINIT, NULL, "can't follow link to group")

                /* Open group */
                if(NULL == (grp = (H5VL_rados_group_t *)H5VL_rados_group_open_helper(item->file, oid, gapl_id, dxpl_id, req, (collective && (item->file->num_procs > 1)) ? (void **)&gcpl_buf : NULL, &gcpl_len)))
                    HGOTO_ERROR(H5E_SYM, H5E_CANTOPENOBJ, NULL, "can't open group")
            } /* end else */
        } /* end else */

        /* Broadcast group info if there are other processes that need it */
        if(collective && (item->file->num_procs > 1)) {
            HDassert(gcpl_buf);
            HDassert(sizeof(ginfo_buf_static) >= 2 * sizeof(uint64_t));

            /* Encode oid */
            p = ginfo_buf_static;
            UINT64ENCODE(p, grp->obj.bin_oid)

            /* Encode GCPL length */
            UINT64ENCODE(p, gcpl_len)

            /* Copy GCPL to ginfo_buf_static if it will fit */
            if((gcpl_len + 2 * sizeof(uint64_t)) <= sizeof(ginfo_buf_static))
                (void)HDmemcpy(p, gcpl_buf, gcpl_len);

            /* We are about to bcast so we no longer need to bcast on failure */
            must_bcast = FALSE;

            /* MPI_Bcast ginfo_buf */
            if(MPI_SUCCESS != MPI_Bcast((char *)ginfo_buf_static, sizeof(ginfo_buf_static), MPI_BYTE, 0, item->file->comm))
                HGOTO_ERROR(H5E_SYM, H5E_MPI, NULL, "can't bcast group info")

            /* Need a second bcast if it did not fit in the receivers' static
             * buffers */
            if(gcpl_len + 2 * sizeof(uint64_t) > sizeof(ginfo_buf_static))
                if(MPI_SUCCESS != MPI_Bcast((char *)gcpl_buf, (int)gcpl_len, MPI_BYTE, 0, item->file->comm))
                    HGOTO_ERROR(H5E_SYM, H5E_MPI, NULL, "can't bcast GCPL")
        } /* end if */
    } /* end if */
    else {
        /* Receive GCPL */
        if(MPI_SUCCESS != MPI_Bcast((char *)ginfo_buf_static, sizeof(ginfo_buf_static), MPI_BYTE, 0, item->file->comm))
            HGOTO_ERROR(H5E_SYM, H5E_MPI, NULL, "can't bcast group info")

        /* Decode oid */
        p = ginfo_buf_static;
        UINT64DECODE(p, oid)

        /* Decode GCPL length */
        UINT64DECODE(p, gcpl_len)

        /* Check for gcpl_len set to 0 - indicates failure */
        if(gcpl_len == 0)
            HGOTO_ERROR(H5E_SYM, H5E_CANTINIT, NULL, "lead process failed to open group")

        /* Check if we need to perform another bcast */
        if(gcpl_len + 2 * sizeof(uint64_t) > sizeof(ginfo_buf_static)) {
            /* Allocate a dynamic buffer if necessary */
            if(gcpl_len > sizeof(ginfo_buf_static)) {
                if(NULL == (gcpl_buf = (uint8_t *)H5MM_malloc(gcpl_len)))
                    HGOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, NULL, "can't allocate space for global pool handle")
                p = gcpl_buf;
            } /* end if */
            else
                p = ginfo_buf_static;

            /* Receive GCPL */
            if(MPI_SUCCESS != MPI_Bcast((char *)p, (int)gcpl_len, MPI_BYTE, 0, item->file->comm))
                HGOTO_ERROR(H5E_SYM, H5E_MPI, NULL, "can't bcast GCPL")
        } /* end if */

        /* Reconstitute group from received oid and GCPL buffer */
        if(NULL == (grp = (H5VL_rados_group_t *)H5VL_rados_group_reconstitute(item->file, oid, p, gapl_id, dxpl_id, req)))
            HGOTO_ERROR(H5E_SYM, H5E_CANTINIT, NULL, "can't reconstitute group")
    } /* end else */

    /* Set return value */
    ret_value = (void *)grp;

done:
    /* Cleanup on failure */
    if(NULL == ret_value) {
        /* Bcast gcpl_buf as '0' if necessary - this will trigger failures in
         * other processes so we do not need to do the second bcast. */
        if(must_bcast) {
            HDmemset(ginfo_buf_static, 0, sizeof(ginfo_buf_static));
            if(MPI_SUCCESS != MPI_Bcast(ginfo_buf_static, sizeof(ginfo_buf_static), MPI_BYTE, 0, item->file->comm))
                HDONE_ERROR(H5E_SYM, H5E_MPI, NULL, "can't bcast empty group info")
        } /* end if */

        /* Close group */
        if(grp && H5VL_rados_group_close(grp, dxpl_id, req) < 0)
            HDONE_ERROR(H5E_SYM, H5E_CLOSEERROR, NULL, "can't close group")
    } /* end if */

    /* Close target group */
    if(target_grp && H5VL_rados_group_close(target_grp, dxpl_id, req) < 0)
        HDONE_ERROR(H5E_SYM, H5E_CLOSEERROR, NULL, "can't close group")

    /* Free memory */
    gcpl_buf = (uint8_t *)H5MM_xfree(gcpl_buf);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5VL_rados_group_open() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_rados_group_close
 *
 * Purpose:     Closes a RADOS HDF5 group.
 *
 * Return:      Success:        0
 *              Failure:        -1
 *
 * Programmer:  Neil Fortner
 *              February, 2018
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_rados_group_close(void *_grp, hid_t H5_ATTR_UNUSED dxpl_id,
    void H5_ATTR_UNUSED **req)
{
    H5VL_rados_group_t *grp = (H5VL_rados_group_t *)_grp;
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_NOAPI_NOINIT

    HDassert(grp);

    if(--grp->obj.item.rc == 0) {
        /* Free group data structures */
        grp->obj.oid = (char *)H5MM_xfree(grp->obj.oid);
        if(grp->gcpl_id != FAIL && H5I_dec_app_ref(grp->gcpl_id) < 0)
            HDONE_ERROR(H5E_SYM, H5E_CANTDEC, FAIL, "failed to close plist")
        if(grp->gapl_id != FAIL && H5I_dec_app_ref(grp->gapl_id) < 0)
            HDONE_ERROR(H5E_SYM, H5E_CANTDEC, FAIL, "failed to close plist")
        grp = H5FL_FREE(H5VL_rados_group_t, grp);
    } /* end if */

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5VL_rados_group_close() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_rados_need_bkg
 *
 * Purpose:     Determine if a background buffer is needed for conversion.
 *
 * Return:      Success:        0
 *              Failure:        -1
 *
 * Programmer:  Neil Fortner
 *              March, 2018
 *
 *-------------------------------------------------------------------------
 */
static htri_t
H5VL_rados_need_bkg(hid_t src_type_id, hid_t dst_type_id, size_t *dst_type_size,
    hbool_t *fill_bkg)
{
    hid_t memb_type_id = -1;
    hid_t src_memb_type_id = -1;
    char *memb_name = NULL;
    size_t memb_size;
    H5T_class_t tclass;
    htri_t ret_value;

    FUNC_ENTER_NOAPI_NOINIT

    /* Get destination type size */
    if((*dst_type_size = H5Tget_size(dst_type_id)) == 0)
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTGET, FAIL, "can't get source type size")

    /* Get datatype class */
    if(H5T_NO_CLASS == (tclass = H5Tget_class(dst_type_id)))
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTGET, FAIL, "can't get type class")

    switch(tclass) {
        case H5T_INTEGER:
        case H5T_FLOAT:
        case H5T_TIME:
        case H5T_STRING:
        case H5T_BITFIELD:
        case H5T_OPAQUE:
        case H5T_ENUM:
            /* No background buffer necessary */
            ret_value = FALSE;

            break;

        case H5T_COMPOUND:
            {
                int nmemb;
                size_t size_used = 0;
                int src_i;
                int i;

                /* We must always provide a background buffer for compound
                 * conversions.  Only need to check further to see if it must be
                 * filled. */
                ret_value = TRUE;

                /* Get number of compound members */
                if((nmemb = H5Tget_nmembers(dst_type_id)) < 0)
                    HGOTO_ERROR(H5E_DATATYPE, H5E_CANTGET, FAIL, "can't get number of destination compound members")

                /* Iterate over compound members, checking for a member in
                 * dst_type_id with no match in src_type_id */
                for(i = 0; i < nmemb; i++) {
                    /* Get member type */
                    if((memb_type_id = H5Tget_member_type(dst_type_id, (unsigned)i)) < 0)
                        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTGET, FAIL, "can't get compound member type")

                    /* Get member name */
                    if(NULL == (memb_name = H5Tget_member_name(dst_type_id, (unsigned)i)))
                        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTGET, FAIL, "can't get compound member name")

                    /* Check for matching name in source type */
                    H5E_BEGIN_TRY {
                        src_i = H5Tget_member_index(src_type_id, memb_name);
                    } H5E_END_TRY

                    /* Free memb_name */
                    if(H5free_memory(memb_name) < 0)
                        HGOTO_ERROR(H5E_RESOURCE, H5E_CANTFREE, FAIL, "can't free member name")
                    memb_name = NULL;

                    /* If no match was found, this type is not being filled in,
                     * so we must fill the background buffer */
                    if(src_i < 0) {
                        if(H5Tclose(memb_type_id) < 0)
                            HGOTO_ERROR(H5E_DATATYPE, H5E_CLOSEERROR, FAIL, "can't close member type")
                        memb_type_id = -1;
                        *fill_bkg = TRUE;
                        HGOTO_DONE(TRUE)
                    } /* end if */

                    /* Open matching source type */
                    if((src_memb_type_id = H5Tget_member_type(src_type_id, (unsigned)src_i)) < 0)
                        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTGET, FAIL, "can't get compound member type")

                    /* Recursively check member type, this will fill in the
                     * member size */
                    if(H5VL_rados_need_bkg(src_memb_type_id, memb_type_id, &memb_size, fill_bkg) < 0)
                        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, FAIL, "can't check if background buffer needed")

                    /* Close source member type */
                    if(H5Tclose(src_memb_type_id) < 0)
                        HGOTO_ERROR(H5E_DATATYPE, H5E_CLOSEERROR, FAIL, "can't close member type")
                    src_memb_type_id = -1;

                    /* Close member type */
                    if(H5Tclose(memb_type_id) < 0)
                        HGOTO_ERROR(H5E_DATATYPE, H5E_CLOSEERROR, FAIL, "can't close member type")
                    memb_type_id = -1;

                    /* If the source member type needs the background filled, so
                     * does the parent */
                    if(*fill_bkg)
                        HGOTO_DONE(TRUE)

                    /* Keep track of the size used in compound */
                    size_used += memb_size;
                } /* end for */

                /* Check if all the space in the type is used.  If not, we must
                 * fill the background buffer. */
                /* TODO: This is only necessary on read, we don't care about
                 * compound gaps in the "file" DSMINC */
                HDassert(size_used <= *dst_type_size);
                if(size_used != *dst_type_size)
                    *fill_bkg = TRUE;

                break;
            } /* end block */

        case H5T_ARRAY:
            /* Get parent type */
            if((memb_type_id = H5Tget_super(dst_type_id)) < 0)
                HGOTO_ERROR(H5E_DATATYPE, H5E_CANTGET, FAIL, "can't get array parent type")

            /* Get source parent type */
            if((src_memb_type_id = H5Tget_super(src_type_id)) < 0)
                HGOTO_ERROR(H5E_DATATYPE, H5E_CANTGET, FAIL, "can't get array parent type")

            /* Recursively check parent type */
            if((ret_value = H5VL_rados_need_bkg(src_memb_type_id, memb_type_id, &memb_size, fill_bkg)) < 0)
                HGOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, FAIL, "can't check if background buffer needed")

            /* Close source parent type */
            if(H5Tclose(src_memb_type_id) < 0)
                HGOTO_ERROR(H5E_DATATYPE, H5E_CLOSEERROR, FAIL, "can't close array parent type")
            src_memb_type_id = -1;

            /* Close parent type */
            if(H5Tclose(memb_type_id) < 0)
                HGOTO_ERROR(H5E_DATATYPE, H5E_CLOSEERROR, FAIL, "can't close array parent type")
            memb_type_id = -1;

            break;

        case H5T_REFERENCE:
        case H5T_VLEN:
            /* Not yet supported */
            HGOTO_ERROR(H5E_DATATYPE, H5E_UNSUPPORTED, FAIL, "reference and vlen types not supported")

            break;

        case H5T_NO_CLASS:
        case H5T_NCLASSES:
        default:
            HGOTO_ERROR(H5E_DATATYPE, H5E_BADVALUE, FAIL, "invalid type class")
    } /* end switch */

done:
    /* Cleanup on failure */
    if(ret_value < 0) {
        if(memb_type_id >= 0)
            if(H5I_dec_app_ref(memb_type_id) < 0)
                HDONE_ERROR(H5E_DATATYPE, H5E_CANTDEC, FAIL, "failed to close member type")
        if(src_memb_type_id >= 0)
            if(H5I_dec_app_ref(src_memb_type_id) < 0)
                HDONE_ERROR(H5E_DATATYPE, H5E_CANTDEC, FAIL, "failed to close source member type")
        memb_name = (char *)H5MM_xfree(memb_name);
    } /* end if */

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5VL_rados_need_bkg() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_rados_tconv_init
 *
 * Purpose:     DSMINC
 *
 * Return:      Success:        0
 *              Failure:        -1
 *
 * Programmer:  Neil Fortner
 *              April, 2018
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_rados_tconv_init(hid_t src_type_id, size_t *src_type_size,
    hid_t dst_type_id, size_t *dst_type_size, hbool_t *_types_equal,
    H5VL_rados_tconv_reuse_t *reuse, hbool_t *_need_bkg, hbool_t *fill_bkg)
{
    htri_t need_bkg = FALSE;
    htri_t types_equal;
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_NOAPI_NOINIT

    HDassert(src_type_size);
    HDassert(dst_type_size);
    HDassert(_types_equal);
    HDassert(_need_bkg);
    HDassert(fill_bkg);
    HDassert(!*fill_bkg);

    /* Get source type size */
    if((*src_type_size = H5Tget_size(src_type_id)) == 0)
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTGET, FAIL, "can't get source type size")

    /* Check if the types are equal */
    if((types_equal = H5Tequal(src_type_id, dst_type_id)) < 0)
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTCOMPARE, FAIL, "can't check if types are equal")
    if(types_equal)
        /* Types are equal, no need for conversion, just set dst_type_size */
        *dst_type_size = *src_type_size;
    else {
        /* Check if we need a background buffer */
        if((need_bkg = H5VL_rados_need_bkg(src_type_id, dst_type_id, dst_type_size, fill_bkg)) < 0)
            HGOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, FAIL, "can't check if background buffer needed")

        /* Check for reusable destination buffer */
        if(reuse) {
            HDassert(*reuse == H5VL_RADOS_TCONV_REUSE_NONE);

            /* Use dest buffer for type conversion if it large enough, otherwise
             * use it for the background buffer if one is needed. */
            if(dst_type_size >= src_type_size)
                *reuse = H5VL_RADOS_TCONV_REUSE_TCONV;
            else if(need_bkg)
                *reuse = H5VL_RADOS_TCONV_REUSE_BKG;
        } /* end if */
    } /* end else */

    /* Set return values */
    *_types_equal = types_equal;
    *_need_bkg = need_bkg;

done:
    /* Cleanup on failure */
    if(ret_value < 0) {
        *reuse = H5VL_RADOS_TCONV_REUSE_NONE;
    } /* end if */

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5VL_rados_tconv_init() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_rados_dataset_create
 *
 * Purpose:     Sends a request to RADOS to create a dataset
 *
 * Return:      Success:        dataset object. 
 *              Failure:        NULL
 *
 * Programmer:  Neil Fortner
 *              March, 2018
 *
 *-------------------------------------------------------------------------
 */
static void *
H5VL_rados_dataset_create(void *_item,
    const H5VL_loc_params_t H5_ATTR_UNUSED *loc_params, const char *name,
    hid_t dcpl_id, hid_t dapl_id, hid_t dxpl_id, void **req)
{
    H5VL_rados_item_t *item = (H5VL_rados_item_t *)_item;
    H5VL_rados_dset_t *dset = NULL;
    H5P_genplist_t *plist = NULL;      /* Property list pointer */
    hid_t type_id, space_id;
    H5VL_rados_group_t *target_grp = NULL;
    uint8_t *md_buf = NULL;
    hbool_t collective = item->file->collective;
    int ret;
    void *ret_value = NULL;

    FUNC_ENTER_NOAPI_NOINIT

    /* Check for write access */
    if(!(item->file->flags & H5F_ACC_RDWR))
        HGOTO_ERROR(H5E_FILE, H5E_BADVALUE, NULL, "no write intent on file")
 
    /* Check for collective access, if not already set by the file */
    if(!collective)
        if(H5Pget_all_coll_metadata_ops(dapl_id, &collective) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, NULL, "can't get collective access property")

    /* Get the dcpl plist structure */
    if(NULL == (plist = (H5P_genplist_t *)H5I_object(dcpl_id)))
        HGOTO_ERROR(H5E_ATOM, H5E_BADATOM, NULL, "can't find object for ID")

    /* get creation properties */
    if(H5P_get(plist, H5VL_PROP_DSET_TYPE_ID, &type_id) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, NULL, "can't get property value for datatype id")
    if(H5P_get(plist, H5VL_PROP_DSET_SPACE_ID, &space_id) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, NULL, "can't get property value for space id")

    /* Allocate the dataset object that is returned to the user */
    if(NULL == (dset = H5FL_CALLOC(H5VL_rados_dset_t)))
        HGOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, NULL, "can't allocate RADOS dataset struct")
    dset->obj.item.type = H5I_DATASET;
    dset->obj.item.file = item->file;
    dset->obj.item.rc = 1;
    dset->type_id = FAIL;
    dset->space_id = FAIL;
    dset->dcpl_id = FAIL;
    dset->dapl_id = FAIL;

    /* Generate dataset oid */
    if(H5VL_rados_oid_create(item->file, item->file->max_oid + (uint64_t)1, H5I_DATASET, &dset->obj.bin_oid, &dset->obj.oid) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, NULL, "can't generate dataset oid")

    /* Update max_oid */
    item->file->max_oid = H5VL_rados_oid_to_idx(dset->obj.bin_oid);

    /* Create dataset and write metadata if this process should */
    if(!collective || (item->file->my_rank == 0)) {
        const char *target_name = NULL;
        H5VL_rados_link_val_t link_val;
        uint8_t *p;
        size_t type_size = 0;
        size_t space_size = 0;
        size_t dcpl_size = 0;
        size_t md_size = 0;

        /* Traverse the path */
        if(NULL == (target_grp = H5VL_rados_group_traverse_const(item, name, dxpl_id, req, &target_name, NULL, NULL)))
            HGOTO_ERROR(H5E_DATASET, H5E_BADITER, NULL, "can't traverse path")

        /* Create dataset */

        /* Determine buffer sizes */
        if(H5Tencode(type_id, NULL, &type_size) < 0)
            HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, NULL, "can't determine serialized length of datatype")
        if(H5Sencode(space_id, NULL, &space_size) < 0)
            HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, NULL, "can't determine serialized length of dataaspace")
        if(H5Pencode(dcpl_id, NULL, &dcpl_size) < 0)
            HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, NULL, "can't determine serialized length of dcpl")
        md_size = (3 * sizeof(uint64_t)) + type_size + space_size + dcpl_size;

        /* Allocate metadata buffer */
        if(NULL == (md_buf = (uint8_t *)H5MM_malloc(md_size)))
            HGOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, NULL, "can't allocate buffer for constant metadata")

        /* Encode info lengths */
        p = md_buf;
        UINT64ENCODE(p, (uint64_t)type_size)
        UINT64ENCODE(p, (uint64_t)space_size)
        UINT64ENCODE(p, (uint64_t)dcpl_size)

        /* Encode datatype */
        if(H5Tencode(type_id, md_buf + (3 * sizeof(uint64_t)), &type_size) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTENCODE, NULL, "can't serialize datatype")

        /* Encode dataspace */
        if(H5Sencode(space_id, md_buf + (3 * sizeof(uint64_t)) + type_size, &space_size) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTENCODE, NULL, "can't serialize dataaspace")

        /* Encode DCPL */
        if(H5Pencode(dcpl_id, md_buf + (3 * sizeof(uint64_t)) + type_size + space_size, &dcpl_size) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTENCODE, NULL, "can't serialize dcpl")

        /* Write internal metadata to dataset */
        if((ret = H5VL_rados_write_full(ioctx_g, dset->obj.oid, (const char *)md_buf, md_size)) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, NULL, "can't write metadata to dataset: %s", strerror(-ret))

        /* Mark max OID as dirty */
        item->file->max_oid_dirty = TRUE;

        /* Create link to dataset */
        link_val.type = H5L_TYPE_HARD;
        link_val.target.hard = dset->obj.bin_oid;
        if(H5VL_rados_link_write(target_grp, target_name, &link_val) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, NULL, "can't create link to dataset")
    } /* end if */

    /* Finish setting up dataset struct */
    if((dset->type_id = H5Tcopy(type_id)) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTCOPY, NULL, "failed to copy datatype")
    if((dset->space_id = H5Scopy(space_id)) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTCOPY, NULL, "failed to copy dataspace")
    if(H5Sselect_all(dset->space_id) < 0)
        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTDELETE, NULL, "can't change selection")
    if((dset->dcpl_id = H5Pcopy(dcpl_id)) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTCOPY, NULL, "failed to copy dcpl")
    if((dset->dapl_id = H5Pcopy(dapl_id)) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTCOPY, NULL, "failed to copy dapl")

    /* Set return value */
    ret_value = (void *)dset;

done:
    /* Close target group */
    if(target_grp && H5VL_rados_group_close(target_grp, dxpl_id, req) < 0)
        HDONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, NULL, "can't close group")

    /* Cleanup on failure */
    /* Destroy RADOS object if created before failure DSMINC */
    if(NULL == ret_value)
        /* Close dataset */
        if(dset && H5VL_rados_dataset_close(dset, dxpl_id, req) < 0)
            HDONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, NULL, "can't close dataset")

    /* Free memory */
    md_buf = (uint8_t *)H5MM_xfree(md_buf);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5VL_rados_dataset_create() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_rados_dataset_open
 *
 * Purpose:     Sends a request to RADOS to open a dataset
 *
 * Return:      Success:        dataset object. 
 *              Failure:        NULL
 *
 * Programmer:  Neil Fortner
 *              March, 2016
 *
 *-------------------------------------------------------------------------
 */
static void *
H5VL_rados_dataset_open(void *_item,
    const H5VL_loc_params_t *loc_params, const char *name,
    hid_t dapl_id, hid_t dxpl_id, void **req)
{
    H5VL_rados_item_t *item = (H5VL_rados_item_t *)_item;
    H5VL_rados_dset_t *dset = NULL;
    H5VL_rados_group_t *target_grp = NULL;
    const char *target_name = NULL;
    uint64_t type_len = 0;
    uint64_t space_len = 0;
    uint64_t dcpl_len = 0;
    time_t pmtime = 0;
    uint8_t dinfo_buf_static[H5VL_RADOS_DINFO_BUF_SIZE];
    uint8_t *dinfo_buf_dyn = NULL;
    uint8_t *dinfo_buf = dinfo_buf_static;
    uint8_t *p;
    hbool_t collective = item->file->collective;
    hbool_t must_bcast = FALSE;
    int ret;
    void *ret_value = NULL;

    FUNC_ENTER_NOAPI_NOINIT
 
    /* Check for collective access, if not already set by the file */
    if(!collective)
        if(H5Pget_all_coll_metadata_ops(dapl_id, &collective) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, NULL, "can't get collective access property")

    /* Allocate the dataset object that is returned to the user */
    if(NULL == (dset = H5FL_CALLOC(H5VL_rados_dset_t)))
        HGOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, NULL, "can't allocate RADOS dataset struct")
    dset->obj.item.type = H5I_DATASET;
    dset->obj.item.file = item->file;
    dset->obj.item.rc = 1;
    dset->type_id = FAIL;
    dset->space_id = FAIL;
    dset->dcpl_id = FAIL;
    dset->dapl_id = FAIL;

    /* Check if we're actually opening the group or just receiving the dataset
     * info from the leader */
    if(!collective || (item->file->my_rank == 0)) {
        uint64_t md_len = 0;

        if(collective && (item->file->num_procs > 1))
            must_bcast = TRUE;

        /* Check for open by address */
        if(H5VL_OBJECT_BY_ADDR == loc_params->type) {
            /* Generate oid from address */
            dset->obj.bin_oid = (uint64_t)loc_params->loc_data.loc_by_addr.addr;
            if(H5VL_rados_oid_create_string(item->file, dset->obj.bin_oid, &dset->obj.oid) < 0)
                HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, NULL, "can't encode string oid")
        } /* end if */
        else {
            /* Open using name parameter */
            /* Traverse the path */
            if(NULL == (target_grp = H5VL_rados_group_traverse_const(item, name, dxpl_id, req, &target_name, NULL, NULL)))
                HGOTO_ERROR(H5E_DATASET, H5E_BADITER, NULL, "can't traverse path")

            /* Follow link to dataset */
            if(H5VL_rados_link_follow(target_grp, target_name, dxpl_id, req, &dset->obj.bin_oid) < 0)
                HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, NULL, "can't follow link to dataset")

            /* Create string oid */
            if(H5VL_rados_oid_create_string(item->file, dset->obj.bin_oid, &dset->obj.oid) < 0)
                HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, NULL, "can't encode string oid")
        } /* end else */

        /* Get the object size and time */
        if((ret = H5VL_rados_stat(ioctx_g, dset->obj.oid, &md_len, &pmtime)) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTDECODE, NULL, "can't read metadata size from group: %s", strerror(-ret))

        /* Check for metadata not found */
        if(md_len == (uint64_t)0)
            HGOTO_ERROR(H5E_DATASET, H5E_NOTFOUND, NULL, "internal metadata not found")

        /* Allocate dynamic buffer if necessary */
        if(md_len + sizeof(uint64_t) > sizeof(dinfo_buf_static)) {
            if(NULL == (dinfo_buf_dyn = (uint8_t *)H5MM_malloc(md_len + sizeof(uint64_t))))
                HGOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, NULL, "can't allocate buffer for constant dataset metadata")
            dinfo_buf = dinfo_buf_dyn;
        } /* end if */

        /* Read internal metadata from dataset */
        if((ret = H5VL_rados_read(ioctx_g, dset->obj.oid, (char *)(dinfo_buf + sizeof(uint64_t)), md_len, 0)) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTDECODE, NULL, "can't read metadata from dataset: %s", strerror(-ret))

        /* Decode info lengths */
        p = (uint8_t *)dinfo_buf + sizeof(uint64_t);
        UINT64DECODE(p, type_len)
        UINT64DECODE(p, space_len)
        UINT64DECODE(p, dcpl_len)
        if(type_len + space_len + dcpl_len + (3 * sizeof(uint64_t)) != md_len)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTDECODE, NULL, "dataset internal metadata size mismatch")

        /* Broadcast dataset info if there are other processes that need it */
        if(collective && (item->file->num_procs > 1)) {
            HDassert(dinfo_buf);
            HDassert(sizeof(dinfo_buf_static) >= 4 * sizeof(uint64_t));

            /* Encode oid */
            p = dinfo_buf;
            UINT64ENCODE(p, dset->obj.bin_oid)

            /* MPI_Bcast dinfo_buf */
            HDassert((md_len + sizeof(uint64_t) >= sizeof(dinfo_buf_static)) || (dinfo_buf == dinfo_buf_static));
            if(MPI_SUCCESS != MPI_Bcast((char *)dinfo_buf, sizeof(dinfo_buf_static), MPI_BYTE, 0, item->file->comm))
                HGOTO_ERROR(H5E_DATASET, H5E_MPI, NULL, "can't bcast dataset info")

            /* Need a second bcast if it did not fit in the receivers' static
             * buffer */
            if(dinfo_buf != dinfo_buf_static) {
                HDassert(md_len + sizeof(uint64_t) > sizeof(dinfo_buf_static));
                if(MPI_SUCCESS != MPI_Bcast((char *)dinfo_buf + (4 * sizeof(uint64_t)), (int)(md_len - (3 * sizeof(uint64_t))), MPI_BYTE, 0, item->file->comm))
                    HGOTO_ERROR(H5E_DATASET, H5E_MPI, NULL, "can't bcast dataset info (second bcast)")
            } /* end if */

            /* Reset p */
            p = dinfo_buf + (4 * sizeof(uint64_t));
        } /* end if */
    } /* end if */
    else {
        uint64_t tot_len = 0;

        /* Receive dataset info */
        if(MPI_SUCCESS != MPI_Bcast((char *)dinfo_buf, sizeof(dinfo_buf_static), MPI_BYTE, 0, item->file->comm))
            HGOTO_ERROR(H5E_DATASET, H5E_MPI, NULL, "can't bcast dataset info")

        /* Decode oid */
        p = dinfo_buf_static;
        UINT64DECODE(p, dset->obj.bin_oid)

        /* Decode serialized info lengths */
        UINT64DECODE(p, type_len)
        UINT64DECODE(p, space_len)
        UINT64DECODE(p, dcpl_len)
        tot_len = type_len + space_len + dcpl_len;

        /* Check for type_len set to 0 - indicates failure */
        if(type_len == 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, NULL, "lead process failed to open dataset")

        /* Check if we need to perform another bcast */
        if(tot_len + (4 * sizeof(uint64_t)) > sizeof(dinfo_buf_static)) {
            /* Allocate a dynamic buffer if necessary */
            if(tot_len > sizeof(dinfo_buf_static)) {
                if(NULL == (dinfo_buf_dyn = (uint8_t *)H5MM_malloc(tot_len)))
                    HGOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, NULL, "can't allocate space for dataset info")
                dinfo_buf = dinfo_buf_dyn;
            } /* end if */

            /* Receive dataset info */
            if(MPI_SUCCESS != MPI_Bcast((char *)dinfo_buf, (int)tot_len, MPI_BYTE, 0, item->file->comm))
                HGOTO_ERROR(H5E_DATASET, H5E_MPI, NULL, "can't bcast dataset info (second bcast)")

            p = dinfo_buf;
        } /* end if */
    } /* end else */

    /* Decode datatype, dataspace, and DCPL */
    if((dset->type_id = H5Tdecode(p)) < 0)
        HGOTO_ERROR(H5E_ARGS, H5E_CANTDECODE, NULL, "can't deserialize datatype")
    p += type_len;
    if((dset->space_id = H5Sdecode(p)) < 0)
        HGOTO_ERROR(H5E_ARGS, H5E_CANTDECODE, NULL, "can't deserialize datatype")
    if(H5Sselect_all(dset->space_id) < 0)
        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTDELETE, NULL, "can't change selection")
    p += space_len;
    if((dset->dcpl_id = H5Pdecode(p)) < 0)
        HGOTO_ERROR(H5E_ARGS, H5E_CANTDECODE, NULL, "can't deserialize dataset creation property list")

    /* Finish setting up dataset struct */
    if((dset->dapl_id = H5Pcopy(dapl_id)) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTCOPY, NULL, "failed to copy dapl");

    /* Set return value */
    ret_value = (void *)dset;

done:
    /* Cleanup on failure */
    if(NULL == ret_value) {
        /* Bcast dinfo_buf as '0' if necessary - this will trigger failures in
         * in other processes so we do not need to do the second bcast. */
        if(must_bcast) {
            HDmemset(dinfo_buf_static, 0, sizeof(dinfo_buf_static));
            if(MPI_SUCCESS != MPI_Bcast(dinfo_buf_static, sizeof(dinfo_buf_static), MPI_BYTE, 0, item->file->comm))
                HDONE_ERROR(H5E_DATASET, H5E_MPI, NULL, "can't bcast empty dataset info")
        } /* end if */

        /* Close dataset */
        if(dset && H5VL_rados_dataset_close(dset, dxpl_id, req) < 0)
            HDONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, NULL, "can't close dataset")
    } /* end if */

    /* Close target group */
    if(target_grp && H5VL_rados_group_close(target_grp, dxpl_id, req) < 0)
        HDONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, NULL, "can't close group")

    /* Free memory */
    dinfo_buf_dyn = (uint8_t *)H5MM_xfree(dinfo_buf_dyn);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5VL_rados_dataset_open() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_rados_get_selected_chunk_info
 *
 * Purpose:     Calculates the starting coordinates for the chunks selected
 *              in the file space given by file_space_id and sets up
 *              individual memory and file spaces for each chunk. The chunk
 *              coordinates and dataspaces are returned through the
 *              chunk_info struct pointer.
 *
 *              XXX: Note that performance could be increased by
 *                   calculating all of the chunks in the entire dataset
 *                   and then caching them in the dataset object for
 *                   re-use in subsequent reads/writes
 *
 * Return:      Success: 0
 *              Failure: -1
 *
 * Programmer:  Neil Fortner
 *              May, 2018
 *              Based on H5VL_daosm_get_selected_chunk_info by Jordan
 *              Henderson, May, 2017
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_rados_get_selected_chunk_info(hid_t dcpl,
    hid_t file_space_id, hid_t mem_space_id,
    H5VL_rados_select_chunk_info_t **chunk_info, size_t *chunk_info_len)
{
    H5VL_rados_select_chunk_info_t *_chunk_info = NULL;
    hssize_t  num_sel_points;
    hssize_t  chunk_file_space_adjust[H5O_LAYOUT_NDIMS];
    hsize_t   chunk_dims[H5S_MAX_RANK];
    hsize_t   file_sel_start[H5S_MAX_RANK], file_sel_end[H5S_MAX_RANK];
    hsize_t   mem_sel_start[H5S_MAX_RANK], mem_sel_end[H5S_MAX_RANK];
    hsize_t   start_coords[H5O_LAYOUT_NDIMS], end_coords[H5O_LAYOUT_NDIMS];
    hsize_t   selection_start_coords[H5O_LAYOUT_NDIMS];
    hsize_t   num_sel_points_cast;
    htri_t    space_same_shape = FALSE;
    size_t    info_buf_alloced;
    size_t    i, j;
    H5S_t    *fspace = NULL, *mspace = NULL;
    int       fspace_ndims, mspace_ndims;
    int       increment_dim;
    herr_t    ret_value = SUCCEED;

    FUNC_ENTER_NOAPI_NOINIT

    HDassert(chunk_info);
    HDassert(chunk_info_len);

    if ((num_sel_points = H5Sget_select_npoints(file_space_id)) < 0)
        HGOTO_ERROR(H5E_DATASPACE, H5E_BADVALUE, FAIL, "can't get number of points select in dataspace")
    H5_CHECKED_ASSIGN(num_sel_points_cast, hsize_t, num_sel_points, hssize_t);

    /* Get the chunking information */
    if (H5Pget_chunk(dcpl, H5S_MAX_RANK, chunk_dims) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "can't get chunking information")

    if ((fspace_ndims = H5Sget_simple_extent_ndims(file_space_id)) < 0)
        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTGET, FAIL, "can't get file space dimensionality")
    if ((mspace_ndims = H5Sget_simple_extent_ndims(mem_space_id)) < 0)
        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTGET, FAIL, "can't get memory space dimensionality")
    HDassert(mspace_ndims == fspace_ndims);

    /* Get the bounding box for the current selection in the file space */
    if (H5Sget_select_bounds(file_space_id, file_sel_start, file_sel_end) < 0)
        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTGET, FAIL, "can't get bounding box for file selection")

    if (H5Sget_select_bounds(mem_space_id, mem_sel_start, mem_sel_end) < 0)
        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTGET, FAIL, "can't get bounding box for memory selection")

    /* Calculate the adjustment for memory selection from the file selection */
    for (i = 0; i < (size_t) fspace_ndims; i++) {
        H5_CHECK_OVERFLOW(file_sel_start[i], hsize_t, hssize_t);
        H5_CHECK_OVERFLOW(mem_sel_start[i], hsize_t, hssize_t);
        chunk_file_space_adjust[i] = (hssize_t) file_sel_start[i] - (hssize_t) mem_sel_start[i];
    } /* end for */

    if (NULL == (_chunk_info = (H5VL_rados_select_chunk_info_t *) H5MM_malloc(H5VL_RADOS_DEFAULT_NUM_SEL_CHUNKS * sizeof(*_chunk_info))))
        HGOTO_ERROR(H5E_DATASET, H5E_CANTALLOC, FAIL, "can't allocate space for selected chunk info buffer")
    info_buf_alloced = H5VL_RADOS_DEFAULT_NUM_SEL_CHUNKS * sizeof(*_chunk_info);

    /* Calculate the coordinates for the initial chunk */
    for (i = 0; i < (size_t) fspace_ndims; i++) {
        start_coords[i] = selection_start_coords[i] = (file_sel_start[i] / chunk_dims[i]) * chunk_dims[i];
        end_coords[i] = (start_coords[i] + chunk_dims[i]) - 1;
    } /* end for */

    if (NULL == (fspace = (H5S_t *) H5I_object_verify(file_space_id, H5I_DATASPACE)))
        HGOTO_ERROR(H5E_DATASPACE, H5E_BADVALUE, FAIL, "not a dataspace")
    if (NULL == (mspace = (H5S_t *) H5I_object_verify(mem_space_id, H5I_DATASPACE)))
        HGOTO_ERROR(H5E_DATASPACE, H5E_BADVALUE, FAIL, "not a dataspace")

    if (FAIL == (space_same_shape = H5S_select_shape_same(fspace, mspace)))
        HGOTO_ERROR(H5E_DATASPACE, H5E_BADVALUE, FAIL, "not a dataspace")

    /* Iterate through each "chunk" in the dataset */
    for (i = 0; num_sel_points_cast;) {
        /* Check for intersection of file selection and "chunk". If there is
         * an intersection, set up a valid memory and file space for the chunk. */
        if (TRUE == H5S_hyper_intersect_block(fspace, start_coords, end_coords)) {
            hssize_t  chunk_mem_space_adjust[H5O_LAYOUT_NDIMS];
            hssize_t  chunk_sel_npoints;
            hid_t     tmp_chunk_fspace_id;
            H5S_t    *tmp_chunk_fspace = NULL;

            /* Re-allocate selected chunk info buffer if necessary */
            while (i > (info_buf_alloced / sizeof(*_chunk_info)) - 1) {
                if (NULL == (_chunk_info = (H5VL_rados_select_chunk_info_t *) H5MM_realloc(_chunk_info, 2 * info_buf_alloced)))
                    HGOTO_ERROR(H5E_DATASET, H5E_CANTALLOC, FAIL, "can't reallocate space for selected chunk info buffer")
                info_buf_alloced *= 2;
            } /* end while */

            /*
             * Set up the file Dataspace for this chunk.
             */

            /* Create temporary chunk for selection operations */
            if ((tmp_chunk_fspace_id = H5Scopy(file_space_id)) < 0)
                HGOTO_ERROR(H5E_DATASPACE, H5E_CANTCOPY, FAIL, "unable to copy file space")

            if (NULL == (tmp_chunk_fspace = (H5S_t *) H5I_object_verify(tmp_chunk_fspace_id, H5I_DATASPACE)))
                HGOTO_ERROR(H5E_DATASPACE, H5E_BADVALUE, FAIL, "not a dataspace")

            /* Make certain selections are stored in span tree form (not "optimized hyperslab" or "all") */
            if (H5S_hyper_convert(tmp_chunk_fspace) < 0) {
                (void) H5S_close(tmp_chunk_fspace);
                HGOTO_ERROR(H5E_DATASPACE, H5E_CANTINIT, FAIL, "unable to convert selection to span trees")
            } /* end if */

            /* "AND" temporary chunk and current chunk */
            if (H5S_select_hyperslab(tmp_chunk_fspace, H5S_SELECT_AND, start_coords, NULL, chunk_dims, NULL) < 0) {
                (void) H5S_close(tmp_chunk_fspace);
                HGOTO_ERROR(H5E_DATASPACE, H5E_CANTSELECT, FAIL, "can't create chunk selection")
            } /* end if */

            /* Resize chunk's dataspace dimensions to size of chunk */
            if (H5S_set_extent_real(tmp_chunk_fspace, chunk_dims) < 0) {
                (void) H5S_close(tmp_chunk_fspace);
                HGOTO_ERROR(H5E_DATASPACE, H5E_CANTSELECT, FAIL, "can't adjust chunk dimensions")
            } /* end if */

            /* Move selection back to have correct offset in chunk */
            if (H5S_SELECT_ADJUST_U(tmp_chunk_fspace, start_coords) < 0) {
                (void) H5S_close(tmp_chunk_fspace);
                HGOTO_ERROR(H5E_DATASPACE, H5E_CANTSELECT, FAIL, "can't adjust chunk selection")
            } /* end if */

            /* Copy the chunk's coordinates to the selected chunk info buffer */
            HDmemcpy(_chunk_info[i].chunk_coords, start_coords, (size_t) fspace_ndims * sizeof(hsize_t));

            _chunk_info[i].fspace_id = tmp_chunk_fspace_id;

            /*
             * Now set up the memory Dataspace for this chunk.
             */
            if (space_same_shape) {
                hid_t  tmp_chunk_mspace_id;
                H5S_t *tmp_chunk_mspace = NULL;

                if ((tmp_chunk_mspace_id = H5Scopy(mem_space_id)) < 0)
                    HGOTO_ERROR(H5E_DATASPACE, H5E_CANTCOPY, FAIL, "unable to copy memory space")

                if (NULL == (tmp_chunk_mspace = (H5S_t *) H5I_object_verify(tmp_chunk_mspace_id, H5I_DATASPACE)))
                    HGOTO_ERROR(H5E_DATASPACE, H5E_BADVALUE, FAIL, "not a dataspace")

                /* Release the current selection */
                if (H5S_SELECT_RELEASE(tmp_chunk_mspace) < 0) {
                    (void) H5S_close(tmp_chunk_mspace);
                    HGOTO_ERROR(H5E_DATASPACE, H5E_CANTRELEASE, FAIL, "unable to release selection")
                } /* end if */

                /* Copy the chunk's file space selection to its memory space selection */
                if (H5S_select_copy(tmp_chunk_mspace, tmp_chunk_fspace, FALSE) < 0) {
                    (void) H5S_close(tmp_chunk_mspace);
                    HGOTO_ERROR(H5E_DATASPACE, H5E_CANTCOPY, FAIL, "unable to copy selection")
                } /* end if */

                /* Compute the adjustment for the chunk */
                for (j = 0; j < (size_t) fspace_ndims; j++) {
                    H5_CHECK_OVERFLOW(_chunk_info[i].chunk_coords[j], hsize_t, hssize_t);
                    chunk_mem_space_adjust[j] = chunk_file_space_adjust[j] - (hssize_t) _chunk_info[i].chunk_coords[j];
                } /* end for */

                /* Adjust the selection */
                if (H5S_hyper_adjust_s(tmp_chunk_mspace, chunk_mem_space_adjust) < 0) {
                    (void) H5S_close(tmp_chunk_mspace);
                    HGOTO_ERROR(H5E_DATASPACE, H5E_CANTSELECT, FAIL, "can't adjust chunk memory space selection")
                } /* end if */

                _chunk_info[i].mspace_id = tmp_chunk_mspace_id;
            } /* end if */
            else {
                HGOTO_ERROR(H5E_ARGS, H5E_UNSUPPORTED, FAIL, "file and memory selections must currently have the same shape")
            } /* end else */

            i++;

            /* Determine if there are more chunks to process */
            if ((chunk_sel_npoints = H5S_GET_SELECT_NPOINTS(tmp_chunk_fspace)) < 0)
                HGOTO_ERROR(H5E_DATASPACE, H5E_CANTGET, FAIL, "can't get number of points selected in chunk file space")

            num_sel_points_cast -= (hsize_t) chunk_sel_npoints;

            if (num_sel_points_cast == 0)
                HGOTO_DONE(SUCCEED)
        } /* end if */

        /* Set current increment dimension */
        increment_dim = fspace_ndims - 1;

        /* Increment chunk location in fastest changing dimension */
        H5_CHECK_OVERFLOW(chunk_dims[increment_dim], hsize_t, hssize_t);
        start_coords[increment_dim] += chunk_dims[increment_dim];
        end_coords[increment_dim] += chunk_dims[increment_dim];

        /* Bring chunk location back into bounds, if necessary */
        if (start_coords[increment_dim] > file_sel_end[increment_dim]) {
            do {
                /* Reset current dimension's location to 0 */
                start_coords[increment_dim] = selection_start_coords[increment_dim];
                end_coords[increment_dim] = (start_coords[increment_dim] + chunk_dims[increment_dim]) - 1;

                /* Decrement current dimension */
                increment_dim--;

                /* Increment chunk location in current dimension */
                start_coords[increment_dim] += chunk_dims[increment_dim];
                end_coords[increment_dim] = (start_coords[increment_dim] + chunk_dims[increment_dim]) - 1;
            } while (start_coords[increment_dim] > file_sel_end[increment_dim]);
        } /* end if */
    } /* end for */

done:
    if (ret_value >= 0) {
        *chunk_info = _chunk_info;
        *chunk_info_len = i;
    } /* end if */
    else {
        if (_chunk_info)
            H5MM_free(_chunk_info);
    } /* end else */

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5VL_rados_get_selected_chunk_info() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_rados_build_io_op_merge
 *
 * Purpose:     RADOSINC
 *
 * Return:      Success:        0
 *              Failure:        -1
 *
 * Programmer:  Neil Fortner
 *              April, 2018
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_rados_build_io_op_merge(H5S_t *mem_space, H5S_t *file_space,
    size_t type_size, size_t tot_nelem, void *rbuf, const void *wbuf,
    rados_read_op_t read_op, rados_write_op_t write_op)
{
    H5S_sel_iter_t mem_sel_iter;    /* Selection iteration info */
    hbool_t mem_sel_iter_init = FALSE;      /* Selection iteration info has been initialized */
    H5S_sel_iter_t file_sel_iter;    /* Selection iteration info */
    hbool_t file_sel_iter_init = FALSE;      /* Selection iteration info has been initialized */
    size_t mem_nseq = 0;
    size_t file_nseq = 0;
    size_t nelem;
    hsize_t mem_off[H5VL_RADOS_SEQ_LIST_LEN];
    size_t mem_len[H5VL_RADOS_SEQ_LIST_LEN];
    hsize_t file_off[H5VL_RADOS_SEQ_LIST_LEN];
    size_t file_len[H5VL_RADOS_SEQ_LIST_LEN];
    size_t io_len;
    size_t tot_len = tot_nelem * type_size;
    size_t mem_i = 0;
    size_t file_i = 0;
    size_t mem_ei = 0;
    size_t file_ei = 0;
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_NOAPI_NOINIT

    HDassert(!rbuf != !wbuf);
    HDassert(tot_nelem > 0);

    /* Initialize selection iterators  */
    if(H5S_select_iter_init(&mem_sel_iter, mem_space, type_size) < 0)
        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTINIT, FAIL, "unable to initialize selection iterator")
    mem_sel_iter_init = TRUE;       /* Selection iteration info has been initialized */
    if(H5S_select_iter_init(&file_sel_iter, file_space, type_size) < 0)
        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTINIT, FAIL, "unable to initialize selection iterator")
    file_sel_iter_init = TRUE;       /* Selection iteration info has been initialized */

    /* Generate sequences from the file space until finished */
    do {
        /* Get the sequences of bytes if necessary */
        HDassert(mem_i <= mem_nseq);
        if(mem_i == mem_nseq) {
            if(H5S_SELECT_GET_SEQ_LIST(mem_space, 0, &mem_sel_iter, (size_t)H5VL_RADOS_SEQ_LIST_LEN, (size_t)-1, &mem_nseq, &nelem, mem_off, mem_len) < 0)
                HGOTO_ERROR(H5E_DATASPACE, H5E_CANTGET, FAIL, "sequence length generation failed")
            mem_i = 0;
        } /* end if */
        HDassert(file_i <= file_nseq);
        if(file_i == file_nseq) {
            if(H5S_SELECT_GET_SEQ_LIST(file_space, 0, &file_sel_iter, (size_t)H5VL_RADOS_SEQ_LIST_LEN, (size_t)-1, &file_nseq, &nelem, file_off, file_len) < 0)
                HGOTO_ERROR(H5E_DATASPACE, H5E_CANTGET, FAIL, "sequence length generation failed")
            file_i = 0;
        } /* end if */

        /* Calculate number of elements to put in next merged offset/length
         * pair */
        io_len = mem_len[mem_i] <= file_len[file_i] ? mem_len[mem_i] : file_len[file_i];

        /* Add to I/O op */
        if(rbuf)
            rados_read_op_read(read_op, (uint64_t)(file_off[file_i] + file_ei),
                    io_len, (char *)rbuf + mem_off[mem_i] + mem_ei, NULL, NULL);
        else
            rados_write_op_write(write_op,
                    (const char *)wbuf + mem_off[mem_i] + mem_ei,
                    io_len, (uint64_t)(file_off[file_i] + file_ei));

        /* Update indices */
        if(io_len == mem_len[mem_i]) {
            mem_i++;
            mem_ei = 0;
        } /* end if */
        else {
            HDassert(mem_len[mem_i] > io_len);
            mem_len[mem_i] -= io_len;
            mem_ei += io_len;
        } /* end else */
        if(io_len == file_len[file_i]) {
            file_i++;
            file_ei = 0;
        } /* end if */
        else {
            HDassert(file_len[file_i] > io_len);
            file_len[file_i] -= io_len;
            file_ei += io_len;
        } /* end else */
        tot_len -= io_len;
    } while(tot_len > 0);

done:
    /* Release selection iterators */
    if(mem_sel_iter_init && H5S_SELECT_ITER_RELEASE(&mem_sel_iter) < 0)
        HDONE_ERROR(H5E_DATASPACE, H5E_CANTRELEASE, FAIL, "unable to release selection iterator")
    if(file_sel_iter_init && H5S_SELECT_ITER_RELEASE(&file_sel_iter) < 0)
        HDONE_ERROR(H5E_DATASPACE, H5E_CANTRELEASE, FAIL, "unable to release selection iterator")

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5VL_rados_build_io_op_merge() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_rados_build_io_op_match
 *
 * Purpose:     RADOSINC
 *
 * Return:      Success:        0
 *              Failure:        -1
 *
 * Programmer:  Neil Fortner
 *              April, 2018
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_rados_build_io_op_match(H5S_t *file_space, size_t type_size,
    size_t tot_nelem, void *rbuf, const void *wbuf, rados_read_op_t read_op,
    rados_write_op_t write_op)
{
    H5S_sel_iter_t sel_iter;    /* Selection iteration info */
    hbool_t sel_iter_init = FALSE;      /* Selection iteration info has been initialized */
    size_t nseq;
    size_t nelem;
    hsize_t off[H5VL_RADOS_SEQ_LIST_LEN];
    size_t len[H5VL_RADOS_SEQ_LIST_LEN];
    size_t szi;
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_NOAPI_NOINIT

    HDassert(!rbuf != !wbuf);
    HDassert(tot_nelem > 0);

    /* Initialize selection iterator  */
    if(H5S_select_iter_init(&sel_iter, file_space, type_size) < 0)
        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTINIT, FAIL, "unable to initialize selection iterator")
    sel_iter_init = TRUE;       /* Selection iteration info has been initialized */

    /* Generate sequences from the file space until finished */
    do {
        /* Get the sequences of bytes */
        if(H5S_SELECT_GET_SEQ_LIST(file_space, 0, &sel_iter, (size_t)H5VL_RADOS_SEQ_LIST_LEN, (size_t)-1, &nseq, &nelem, off, len) < 0)
            HGOTO_ERROR(H5E_DATASPACE, H5E_CANTGET, FAIL, "sequence length generation failed")
        tot_nelem -= nelem;

        /* Create io ops from offsets and lengths */
        if(rbuf)
            for(szi = 0; szi < nseq; szi++)
                rados_read_op_read(read_op, (uint64_t)off[szi], len[szi],
                        (char *)rbuf + off[szi], NULL, NULL);
        else
            for(szi = 0; szi < nseq; szi++)
                rados_write_op_write(write_op, (const char *)wbuf + off[szi],
                        len[szi], (uint64_t)off[szi]);
    } while(tot_nelem > 0);

done:
    /* Release selection iterator */
    if(sel_iter_init && H5S_SELECT_ITER_RELEASE(&sel_iter) < 0)
        HDONE_ERROR(H5E_DATASPACE, H5E_CANTRELEASE, FAIL, "unable to release selection iterator")

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5VL_rados_build_io_op_match() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_rados_build_io_op_contig
 *
 * Purpose:     RADOSINC
 *
 * Return:      Success:        0
 *              Failure:        -1
 *
 * Programmer:  Neil Fortner
 *              April, 2018
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_rados_build_io_op_contig(H5S_t *file_space, size_t type_size,
    size_t tot_nelem, void *rbuf, const void *wbuf, rados_read_op_t read_op,
    rados_write_op_t write_op)
{
    H5S_sel_iter_t sel_iter;    /* Selection iteration info */
    hbool_t sel_iter_init = FALSE;      /* Selection iteration info has been initialized */
    size_t nseq;
    size_t nelem;
    hsize_t off[H5VL_RADOS_SEQ_LIST_LEN];
    size_t len[H5VL_RADOS_SEQ_LIST_LEN];
    size_t mem_off = 0;
    size_t szi;
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_NOAPI_NOINIT

    HDassert(rbuf || wbuf);
    HDassert(tot_nelem > 0);

    /* Initialize selection iterator  */
    if(H5S_select_iter_init(&sel_iter, file_space, type_size) < 0)
        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTINIT, FAIL, "unable to initialize selection iterator")
    sel_iter_init = TRUE;       /* Selection iteration info has been initialized */

    /* Generate sequences from the file space until finished */
    do {
        /* Get the sequences of bytes */
        if(H5S_SELECT_GET_SEQ_LIST(file_space, 0, &sel_iter, (size_t)H5VL_RADOS_SEQ_LIST_LEN, (size_t)-1, &nseq, &nelem, off, len) < 0)
            HGOTO_ERROR(H5E_DATASPACE, H5E_CANTGET, FAIL, "sequence length generation failed")
        tot_nelem -= nelem;

        /* Create io ops from offsets and lengths */
        for(szi = 0; szi < nseq; szi++) {
            if(rbuf)
                rados_read_op_read(read_op, (uint64_t)off[szi], len[szi],
                        (char *)rbuf + mem_off, NULL, NULL);
            if(wbuf)
                rados_write_op_write(write_op, (const char *)wbuf + mem_off,
                        len[szi], (uint64_t)off[szi]);
            mem_off += len[szi];
        } /* end for */
    } while(tot_nelem > 0);

done:
    /* Release selection iterator */
    if(sel_iter_init && H5S_SELECT_ITER_RELEASE(&sel_iter) < 0)
        HDONE_ERROR(H5E_DATASPACE, H5E_CANTRELEASE, FAIL, "unable to release selection iterator")

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5VL_rados_build_io_op_contig() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_rados_scatter_cb
 *
 * Purpose:     Callback function for H5Dscatter.  Simply passes the
 *              entire buffer described by udata to H5Dscatter.
 *
 * Return:      SUCCEED (never fails)
 *
 * Programmer:  Neil Fortner
 *              April, 2018
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_rados_scatter_cb(const void **src_buf, size_t *src_buf_bytes_used,
    void *_udata)
{
    H5VL_rados_scatter_cb_ud_t *udata = (H5VL_rados_scatter_cb_ud_t *)_udata;

    FUNC_ENTER_NOAPI_NOINIT_NOERR

    /* Set src_buf and src_buf_bytes_used to use the entire buffer */
    *src_buf = udata->buf;
    *src_buf_bytes_used = udata->len;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5VL_rados_scatter_cb() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_rados_dataset_read
 *
 * Purpose:     Reads raw data from a dataset into a buffer.
 *`
 * Return:      Success:        0
 *              Failure:        -1, dataset not read.
 *
 * Programmer:  Neil Fortner
 *              April, 2018
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_rados_dataset_read(void *_dset, hid_t mem_type_id, hid_t mem_space_id,
    hid_t file_space_id, hid_t dxpl_id, void *buf, void H5_ATTR_UNUSED **req)
{
    H5VL_rados_select_chunk_info_t *chunk_info = NULL; /* Array of info for each chunk selected in the file */
    H5VL_rados_dset_t *dset = (H5VL_rados_dset_t *)_dset;
    H5S_sel_iter_t sel_iter;    /* Selection iteration info */
    hbool_t sel_iter_init = FALSE;      /* Selection iteration info has been initialized */
    int ndims;
    hsize_t dim[H5S_MAX_RANK];
    hid_t real_file_space_id;
    hid_t real_mem_space_id;
    hssize_t num_elem;
    hssize_t num_elem_chunk;
    size_t chunk_info_len;
    char *chunk_oid = NULL;
    rados_read_op_t read_op;
    hbool_t read_op_init = FALSE;
    size_t file_type_size = 0;
    size_t mem_type_size;
    hbool_t types_equal = TRUE;
    hbool_t need_bkg = FALSE;
    hbool_t fill_bkg = FALSE;
    void *tmp_tconv_buf = NULL;
    void *tmp_bkg_buf = NULL;
    void *tconv_buf;
    void *bkg_buf;
    hbool_t close_spaces = FALSE;
    H5VL_rados_tconv_reuse_t reuse = H5VL_RADOS_TCONV_REUSE_NONE;
    int ret;
    uint64_t i;
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_NOAPI_NOINIT

    /* Get dataspace extent */
    if((ndims = H5Sget_simple_extent_ndims(dset->space_id)) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get number of dimensions")
    if(ndims != H5Sget_simple_extent_dims(dset->space_id, dim, NULL))
        HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get dimensions")

    /* Get "real" file space */
    if(file_space_id == H5S_ALL)
        real_file_space_id = dset->space_id;
    else
        real_file_space_id = file_space_id;

    /* Get number of elements in selection */
    if((num_elem = H5Sget_select_npoints(real_file_space_id)) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get number of points in selection")

    /* Get "real" file space */
    if(mem_space_id == H5S_ALL)
        real_mem_space_id = real_file_space_id;
    else {
        hssize_t num_elem_file;

        real_mem_space_id = mem_space_id;

        /* Verify number of elements in memory selection matches file selection
         */
        if((num_elem_file = H5Sget_select_npoints(real_mem_space_id)) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get number of points in selection")
        if(num_elem_file != num_elem)
            HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "src and dest data spaces have different sizes")
    } /* end else */

    /* Check for no selection */
    if(num_elem == 0)
        HGOTO_DONE(SUCCEED)

    /* Initialize type conversion */
    if(H5VL_rados_tconv_init(dset->type_id, &file_type_size, mem_type_id, &mem_type_size, &types_equal, &reuse, &need_bkg, &fill_bkg) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "can't initialize type conversion")

    /* Check if the dataset actually has a chunked storage layout. If it does not, simply
     * set up the dataset as a single "chunk".
     */
    switch(H5Pget_layout(dset->dcpl_id)) {
        case H5D_COMPACT:
        case H5D_CONTIGUOUS:
            if (NULL == (chunk_info = (H5VL_rados_select_chunk_info_t *)H5MM_malloc(sizeof(H5VL_rados_select_chunk_info_t))))
                HGOTO_ERROR(H5E_DATASET, H5E_CANTALLOC, FAIL, "can't allocate single chunk info buffer")
            chunk_info_len = 1;

            /* Set up "single-chunk dataset", with the "chunk" starting at coordinate 0 */
            chunk_info->fspace_id = real_file_space_id;
            chunk_info->mspace_id = real_mem_space_id;
            HDmemset(chunk_info->chunk_coords, 0, sizeof(chunk_info->chunk_coords));

            break;

        case H5D_CHUNKED:
            /* Get the coordinates of the currently selected chunks in the file, setting up memory and file dataspaces for them */
            if(H5VL_rados_get_selected_chunk_info(dset->dcpl_id, real_file_space_id, real_mem_space_id, &chunk_info, &chunk_info_len) < 0)
                HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get selected chunk info")

            close_spaces = TRUE;

            break;
        case H5D_LAYOUT_ERROR:
        case H5D_NLAYOUTS:
        case H5D_VIRTUAL:
        default:
            HGOTO_ERROR(H5E_DATASET, H5E_UNSUPPORTED, FAIL, "invalid, unknown or unsupported dataset storage layout type")
    } /* end switch */

    /* Get number of elements in a chunk */
    if((num_elem_chunk = H5Sget_simple_extent_npoints(chunk_info[0].fspace_id)) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get number of points in chunk")

    /* Iterate through each of the "chunks" in the dataset */
    for(i = 0; i < chunk_info_len; i++) {
        /* Create read op */
        read_op = rados_create_read_op();
        read_op_init = TRUE;

        /* Create chunk key */
        if(H5VL_rados_oid_create_chunk(dset->obj.item.file, dset->obj.bin_oid, ndims,
                chunk_info[i].chunk_coords, &chunk_oid) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "can't create dataset chunk oid")

        /* Get number of elements in selection */
        if((num_elem = H5Sget_select_npoints(chunk_info[i].mspace_id)) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get number of points in selection")

        /* There was a former if block here... */
        {
            H5S_t *chunk_fspace = NULL;
            H5S_t *chunk_mspace = NULL;
            htri_t match_select = FALSE;

            /* Get file dataspace object */
            if(NULL == (chunk_fspace = (H5S_t *) H5I_object_verify(chunk_info[i].fspace_id, H5I_DATASPACE)))
                HGOTO_ERROR(H5E_DATASPACE, H5E_BADVALUE, FAIL, "not a dataspace")

            /* Check if the types are equal */
            if(types_equal) {
                /* No type conversion necessary */
                /* Check if we should match the file and memory sequence lists
                 * (serialized selections).  We can do this if the memory space
                 * is H5S_ALL and the chunk extent equals the file extent.  If
                 * the number of chunks selected is more than one we do not need
                 * to check the extents because they cannot be the same.  We
                 * could also allow the case where the memory space is not
                 * H5S_ALL but is equivalent. */
                if(mem_space_id == H5S_ALL && chunk_info_len == 1)
                    if((match_select = H5Sextent_equal(real_file_space_id, chunk_info[i].fspace_id)) < 0)
                        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTCOMPARE, FAIL, "can't check if file and chunk dataspaces are equal")

                /* Check for matching selections */
                if(match_select) {
                    /* Build read op from file space */
                    if(H5VL_rados_build_io_op_match(chunk_fspace, file_type_size, (size_t)num_elem, buf, NULL, read_op, NULL) < 0)
                        HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "can't generate RADOS read op")
                } /* end if */
                else {
                    /* Get memory dataspace object */
                    if(NULL == (chunk_mspace = (H5S_t *) H5I_object_verify(chunk_info[i].mspace_id, H5I_DATASPACE)))
                        HGOTO_ERROR(H5E_DATASPACE, H5E_BADVALUE, FAIL, "not a dataspace")

                    /* Build read op from file space and mem space */
                    if(H5VL_rados_build_io_op_merge(chunk_mspace, chunk_fspace, file_type_size, (size_t)num_elem, buf, NULL, read_op, NULL) < 0)
                        HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "can't generate RADOS read op")
                } /* end else */

                /* Read data from dataset */
                if((ret = rados_read_op_operate(read_op, ioctx_g, chunk_oid, LIBRADOS_OPERATION_NOFLAG)) < 0)
                    HGOTO_ERROR(H5E_DATASET, H5E_READERROR, FAIL, "can't read data from dataset: %s", strerror(-ret))
            } /* end if */
            else {
                size_t nseq_tmp;
                size_t nelem_tmp;
                hsize_t sel_off;
                size_t sel_len;
                hbool_t contig;

                /* Type conversion necessary */
                /* Get memory dataspace object */
                if(NULL == (chunk_mspace = (H5S_t *) H5I_object_verify(chunk_info[i].mspace_id, H5I_DATASPACE)))
                    HGOTO_ERROR(H5E_DATASPACE, H5E_BADVALUE, FAIL, "not a dataspace")

                /* Check for contiguous memory buffer */

                /* Initialize selection iterator  */
                if(H5S_select_iter_init(&sel_iter, chunk_mspace, (size_t)1) < 0)
                    HGOTO_ERROR(H5E_DATASPACE, H5E_CANTINIT, FAIL, "unable to initialize selection iterator")
                sel_iter_init = TRUE;       /* Selection iteration info has been initialized */

                /* Get the sequence list - only check the first sequence because we only
                 * care if it is contiguous and if so where the contiguous selection
                 * begins */
                if(H5S_SELECT_GET_SEQ_LIST(chunk_mspace, 0, &sel_iter, (size_t)1, (size_t)-1, &nseq_tmp, &nelem_tmp, &sel_off, &sel_len) < 0)
                    HGOTO_ERROR(H5E_DATASPACE, H5E_CANTGET, FAIL, "sequence length generation failed")
                contig = (sel_len == (size_t)num_elem);
                sel_off *= (hsize_t)mem_type_size;

                /* Release selection iterator */
                if(H5S_SELECT_ITER_RELEASE(&sel_iter) < 0)
                    HGOTO_ERROR(H5E_DATASPACE, H5E_CANTRELEASE, FAIL, "unable to release selection iterator")
                sel_iter_init = FALSE;

                /* Find or allocate usable type conversion buffer */
                if(contig && (reuse == H5VL_RADOS_TCONV_REUSE_TCONV))
                    tconv_buf = (char *)buf + (size_t)sel_off;
                else {
                    if(!tmp_tconv_buf)
                        if(NULL == (tmp_tconv_buf = H5MM_malloc(
                                (size_t)num_elem_chunk * (file_type_size
                                > mem_type_size ? file_type_size
                                : mem_type_size))))
                            HGOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't allocate type conversion buffer")
                    tconv_buf = tmp_tconv_buf;
                } /* end else */

                /* Find or allocate usable background buffer */
                if(need_bkg) {
                    if(contig && (reuse == H5VL_RADOS_TCONV_REUSE_BKG))
                        bkg_buf = (char *)buf + (size_t)sel_off;
                    else {
                        if(!tmp_bkg_buf)
                            if(NULL == (tmp_bkg_buf = H5MM_malloc(
                                    (size_t)num_elem_chunk * mem_type_size)))
                                HGOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't allocate background buffer")
                        bkg_buf = tmp_bkg_buf;
                    } /* end else */
                } /* end if */
                else
                    bkg_buf = NULL;

                /* Build read op from file space */
                if(H5VL_rados_build_io_op_contig(chunk_fspace, file_type_size, (size_t)num_elem, tconv_buf, NULL, read_op, NULL) < 0)
                    HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "can't generate RADOS write op")

                /* Read data from dataset */
                if((ret = rados_read_op_operate(read_op, ioctx_g, chunk_oid, LIBRADOS_OPERATION_NOFLAG)) < 0)
                    HGOTO_ERROR(H5E_DATASET, H5E_READERROR, FAIL, "can't read data from dataset: %s", strerror(-ret))

                /* Gather data to background buffer if necessary */
                if(fill_bkg && (bkg_buf == tmp_bkg_buf))
                    if(H5Dgather(chunk_info[i].mspace_id, buf, mem_type_id, (size_t)num_elem * mem_type_size, bkg_buf, NULL, NULL) < 0)
                        HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "can't gather data to background buffer")

                /* Perform type conversion */
                if(H5Tconvert(dset->type_id, mem_type_id, (size_t)num_elem, tconv_buf, bkg_buf, dxpl_id) < 0)
                    HGOTO_ERROR(H5E_DATASET, H5E_CANTCONVERT, FAIL, "can't perform type conversion")

                /* Scatter data to memory buffer if necessary */
                if(tconv_buf == tmp_tconv_buf) {
                    H5VL_rados_scatter_cb_ud_t scatter_cb_ud;

                    scatter_cb_ud.buf = tconv_buf;
                    scatter_cb_ud.len = (size_t)num_elem * mem_type_size;
                    if(H5Dscatter(H5VL_rados_scatter_cb, &scatter_cb_ud, mem_type_id, chunk_info[i].mspace_id, buf) < 0)
                        HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "can't scatter data to read buffer")
                } /* end if */
            } /* end else */
        } /* end else */

        rados_release_read_op(read_op);
        read_op_init = FALSE;
    } /* end for */

done:
    /* Free memory */
    if(read_op_init)
        rados_release_read_op(read_op);
    H5MM_xfree(chunk_oid);
    H5MM_xfree(tmp_tconv_buf);
    H5MM_xfree(tmp_bkg_buf);

    if(chunk_info) {
        if(close_spaces) {
            for(i = 0; i < chunk_info_len; i++) {
                if(H5Sclose(chunk_info[i].mspace_id) < 0)
                    HDONE_ERROR(H5E_DATASPACE, H5E_CANTCLOSEOBJ, FAIL, "can't close memory space");
                if(H5Sclose(chunk_info[i].fspace_id) < 0)
                    HDONE_ERROR(H5E_DATASPACE, H5E_CANTCLOSEOBJ, FAIL, "can't close file space");
            } /* end for */
        } /* end if */

        H5MM_free(chunk_info);
    } /* end if */

    /* Release selection iterator */
    if(sel_iter_init && H5S_SELECT_ITER_RELEASE(&sel_iter) < 0)
        HDONE_ERROR(H5E_DATASPACE, H5E_CANTRELEASE, FAIL, "unable to release selection iterator")

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5VL_rados_dataset_read() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_rados_dataset_write
 *
 * Purpose:     Writes raw data from a buffer into a dataset.
 *
 * Return:      Success:        0
 *              Failure:        -1, dataset not written.
 *
 * Programmer:  Neil Fortner
 *              March, 2018
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_rados_dataset_write(void *_dset, hid_t mem_type_id, hid_t mem_space_id,
    hid_t file_space_id, hid_t H5_ATTR_UNUSED dxpl_id,
    const void *buf, void H5_ATTR_UNUSED **req)
{
    H5VL_rados_select_chunk_info_t *chunk_info = NULL; /* Array of info for each chunk selected in the file */
    H5VL_rados_dset_t *dset = (H5VL_rados_dset_t *)_dset;
    int ndims;
    hsize_t dim[H5S_MAX_RANK];
    hid_t real_file_space_id;
    hid_t real_mem_space_id;
    hssize_t num_elem;
    hssize_t num_elem_chunk;
    size_t chunk_info_len;
    char *chunk_oid = NULL;
    rados_write_op_t write_op;
    hbool_t write_op_init = FALSE;
    rados_read_op_t read_op;
    hbool_t read_op_init = FALSE;
    size_t file_type_size;
    size_t mem_type_size;
    hbool_t types_equal = TRUE;
    hbool_t need_bkg = FALSE;
    hbool_t fill_bkg = FALSE;
    void *tconv_buf = NULL;
    void *bkg_buf = NULL;
    hbool_t close_spaces = FALSE;
    H5VL_rados_tconv_reuse_t reuse = H5VL_RADOS_TCONV_REUSE_NONE;
    int ret;
    uint64_t i;
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_NOAPI_NOINIT

    /* Check for write access */
    if(!(dset->obj.item.file->flags & H5F_ACC_RDWR))
        HGOTO_ERROR(H5E_FILE, H5E_BADVALUE, FAIL, "no write intent on file")

    /* Get dataspace extent */
    if((ndims = H5Sget_simple_extent_ndims(dset->space_id)) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get number of dimensions")
    if(ndims != H5Sget_simple_extent_dims(dset->space_id, dim, NULL))
        HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get dimensions")

    /* Get "real" file space */
    if(file_space_id == H5S_ALL)
        real_file_space_id = dset->space_id;
    else
        real_file_space_id = file_space_id;

    /* Get number of elements in selection */
    if((num_elem = H5Sget_select_npoints(real_file_space_id)) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get number of points in selection")

    /* Get "real" file space */
    if(mem_space_id == H5S_ALL)
        real_mem_space_id = real_file_space_id;
    else {
        hssize_t num_elem_file;

        real_mem_space_id = mem_space_id;

        /* Verify number of elements in memory selection matches file selection
         */
        if((num_elem_file = H5Sget_select_npoints(real_mem_space_id)) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get number of points in selection")
        if(num_elem_file != num_elem)
            HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "src and dest data spaces have different sizes")
    } /* end else */

    /* Check for no selection */
    if(num_elem == 0)
        HGOTO_DONE(SUCCEED)

    /* Initialize type conversion */
    if(H5VL_rados_tconv_init(dset->type_id, &file_type_size, mem_type_id, &mem_type_size, &types_equal, &reuse, &need_bkg, &fill_bkg) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "can't initialize type conversion")

    /* Check if the dataset actually has a chunked storage layout. If it does not, simply
     * set up the dataset as a single "chunk".
     */
    switch(H5Pget_layout(dset->dcpl_id)) {
        case H5D_COMPACT:
        case H5D_CONTIGUOUS:
            if (NULL == (chunk_info = (H5VL_rados_select_chunk_info_t *)H5MM_malloc(sizeof(H5VL_rados_select_chunk_info_t))))
                HGOTO_ERROR(H5E_DATASET, H5E_CANTALLOC, FAIL, "can't allocate single chunk info buffer")
            chunk_info_len = 1;

            /* Set up "single-chunk dataset", with the "chunk" starting at coordinate 0 */
            chunk_info->fspace_id = real_file_space_id;
            chunk_info->mspace_id = real_mem_space_id;
            HDmemset(chunk_info->chunk_coords, 0, sizeof(chunk_info->chunk_coords));

            break;

        case H5D_CHUNKED:
            /* Get the coordinates of the currently selected chunks in the file, setting up memory and file dataspaces for them */
            if(H5VL_rados_get_selected_chunk_info(dset->dcpl_id, real_file_space_id, real_mem_space_id, &chunk_info, &chunk_info_len) < 0)
                HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get selected chunk info")

            close_spaces = TRUE;

            break;
        case H5D_LAYOUT_ERROR:
        case H5D_NLAYOUTS:
        case H5D_VIRTUAL:
        default:
            HGOTO_ERROR(H5E_DATASET, H5E_UNSUPPORTED, FAIL, "invalid, unknown or unsupported dataset storage layout type")
    } /* end switch */

    /* Get number of elements in a chunk */
    if((num_elem_chunk = H5Sget_simple_extent_npoints(chunk_info[0].fspace_id)) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get number of points in chunk")

    /* Allocate tconv_buf if necessary */
    if(!types_equal)
        if(NULL == (tconv_buf = H5MM_malloc( (size_t)num_elem_chunk
                * (file_type_size > mem_type_size ? file_type_size
                : mem_type_size))))
            HGOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't allocate type conversion buffer")

    /* Allocate bkg_buf if necessary */
    if(need_bkg)
        if(NULL == (bkg_buf = H5MM_malloc((size_t)num_elem_chunk
                * mem_type_size)))
            HGOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't allocate background buffer")

    /* Iterate through each of the "chunks" in the dataset */
    for(i = 0; i < chunk_info_len; i++) {
        /* Create write op */
        write_op = rados_create_write_op();
        write_op_init = TRUE;

        /* Create chunk key */
        if(H5VL_rados_oid_create_chunk(dset->obj.item.file, dset->obj.bin_oid, ndims,
                chunk_info[i].chunk_coords, &chunk_oid) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "can't create dataset chunk oid")

        /* Get number of elements in selection */
        if((num_elem = H5Sget_select_npoints(chunk_info[i].mspace_id)) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get number of points in selection")

        /* Former if block here... */
        {
            H5S_t *chunk_fspace = NULL;
            H5S_t *chunk_mspace = NULL;
            htri_t match_select = FALSE;

            /* Get file dataspace object */
            if(NULL == (chunk_fspace = (H5S_t *) H5I_object_verify(chunk_info[i].fspace_id, H5I_DATASPACE)))
                HGOTO_ERROR(H5E_DATASPACE, H5E_BADVALUE, FAIL, "not a dataspace")

            /* Check if the types are equal */
            if(types_equal) {
                /* No type conversion necessary */
                /* Check if we should match the file and memory sequence lists
                 * (serialized selections).  We can do this if the memory space
                 * is H5S_ALL and the chunk extent equals the file extent.  If
                 * the number of chunks selected is more than one we do not need
                 * to check the extents because they cannot be the same.  We
                 * could also allow the case where the memory space is not
                 * H5S_ALL but is equivalent. */
                if(mem_space_id == H5S_ALL && chunk_info_len == 1)
                    if((match_select = H5Sextent_equal(real_file_space_id, chunk_info[i].fspace_id)) < 0)
                        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTCOMPARE, FAIL, "can't check if file and chunk dataspaces are equal")

                /* Check for matching selections */
                if(match_select) {
                    /* Build write op from file space */
                    if(H5VL_rados_build_io_op_match(chunk_fspace, file_type_size, (size_t)num_elem, NULL, buf, NULL, write_op) < 0)
                        HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "can't generate RADOS write op")
                } /* end if */
                else {
                    /* Get memory dataspace object */
                    if(NULL == (chunk_mspace = (H5S_t *)H5I_object_verify(chunk_info[i].mspace_id, H5I_DATASPACE)))
                        HGOTO_ERROR(H5E_DATASPACE, H5E_BADVALUE, FAIL, "not a dataspace")

                    /* Build write op from file space and mem space */
                    if(H5VL_rados_build_io_op_merge(chunk_mspace, chunk_fspace, file_type_size, (size_t)num_elem, NULL, buf, NULL, write_op) < 0)
                        HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "can't generate RADOS write op")
                } /* end else */
            } /* end if */
            else {
                /* Type conversion necessary */
                /* Check if we need to fill background buffer */
                if(fill_bkg) {
                    HDassert(bkg_buf);

                    /* Create read op */
                    read_op = rados_create_read_op();
                    read_op_init = TRUE;

                    /* Build io ops (to read to bkg_buf and write from tconv_buf)
                     * from file space */
                    if(H5VL_rados_build_io_op_contig(chunk_fspace, file_type_size, (size_t)num_elem, bkg_buf, tconv_buf, read_op, write_op) < 0)
                        HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "can't generate RADOS write op")

                    /* Read data from dataset to background buffer */
                    if((ret = rados_read_op_operate(read_op, ioctx_g, chunk_oid, LIBRADOS_OPERATION_NOFLAG)) < 0)
                        HGOTO_ERROR(H5E_DATASET, H5E_READERROR, FAIL, "can't read data from dataset: %s", strerror(-ret))

                    rados_release_read_op(read_op);
                    read_op_init = FALSE;
                } /* end if */
                else
                    /* Build write op from file space */
                    if(H5VL_rados_build_io_op_contig(chunk_fspace, file_type_size, (size_t)num_elem, NULL, tconv_buf, NULL, write_op) < 0)
                        HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "can't generate RADOS write op")

                /* Gather data to conversion buffer */
                if(H5Dgather(chunk_info[i].mspace_id, buf, mem_type_id, (size_t)num_elem * mem_type_size, tconv_buf, NULL, NULL) < 0)
                    HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "can't gather data to conversion buffer")

                /* Perform type conversion */
                if(H5Tconvert(mem_type_id, dset->type_id, (size_t)num_elem, tconv_buf, bkg_buf, dxpl_id) < 0)
                    HGOTO_ERROR(H5E_DATASET, H5E_CANTCONVERT, FAIL, "can't perform type conversion")
            } /* end else */

            /* Write data to dataset */
            if((ret = rados_write_op_operate(write_op, ioctx_g, chunk_oid, NULL, LIBRADOS_OPERATION_NOFLAG)) < 0)
                HGOTO_ERROR(H5E_DATASET, H5E_WRITEERROR, FAIL, "can't write data to dataset: %s", strerror(-ret))
        } /* end else */

        rados_release_write_op(write_op);
        write_op_init = FALSE;
    } /* end for */

done:
    /* Free memory */
    if(read_op_init)
        rados_release_read_op(read_op);
    if(write_op_init)
        rados_release_write_op(write_op);
    H5MM_xfree(chunk_oid);
    tconv_buf = H5MM_xfree(tconv_buf);
    bkg_buf = H5MM_xfree(bkg_buf);

    if(chunk_info) {
        if(close_spaces) {
            for(i = 0; i < chunk_info_len; i++) {
                if(H5Sclose(chunk_info[i].mspace_id) < 0)
                    HDONE_ERROR(H5E_DATASPACE, H5E_CANTCLOSEOBJ, FAIL, "can't close memory space");
                if(H5Sclose(chunk_info[i].fspace_id) < 0)
                    HDONE_ERROR(H5E_DATASPACE, H5E_CANTCLOSEOBJ, FAIL, "can't close file space");
            } /* end for */
        } /* end if */

        H5MM_free(chunk_info);
    } /* end if */

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5VL_rados_dataset_write() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_rados_dataset_get
 *
 * Purpose:     Gets certain information about a dataset
 *
 * Return:      Success:        0
 *              Failure:        -1
 *
 * Programmer:  Neil Fortner
 *              April, 2018
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5VL_rados_dataset_get(void *_dset, H5VL_dataset_get_t get_type, 
    hid_t H5_ATTR_UNUSED dxpl_id, void H5_ATTR_UNUSED **req, va_list arguments)
{
    H5VL_rados_dset_t *dset = (H5VL_rados_dset_t *)_dset;
    herr_t       ret_value = SUCCEED;    /* Return value */

    FUNC_ENTER_NOAPI_NOINIT

    switch (get_type) {
        case H5VL_DATASET_GET_DCPL:
            {
                hid_t *plist_id = va_arg(arguments, hid_t *);

                /* Retrieve the dataset's creation property list */
                if((*plist_id = H5Pcopy(dset->dcpl_id)) < 0)
                    HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get dset creation property list")

                break;
            } /* end block */
        case H5VL_DATASET_GET_DAPL:
            {
                hid_t *plist_id = va_arg(arguments, hid_t *);

                /* Retrieve the dataset's access property list */
                if((*plist_id = H5Pcopy(dset->dapl_id)) < 0)
                    HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get dset access property list")

                break;
            } /* end block */
        case H5VL_DATASET_GET_SPACE:
            {
                hid_t *ret_id = va_arg(arguments, hid_t *);

                /* Retrieve the dataset's dataspace */
                if((*ret_id = H5Scopy(dset->space_id)) < 0)
                    HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get dataspace ID of dataset");
                break;
            } /* end block */
        case H5VL_DATASET_GET_SPACE_STATUS:
            {
                H5D_space_status_t *allocation = va_arg(arguments, H5D_space_status_t *);

                /* Retrieve the dataset's space status */
                *allocation = H5D_SPACE_STATUS_NOT_ALLOCATED;
                break;
            } /* end block */
        case H5VL_DATASET_GET_TYPE:
            {
                hid_t *ret_id = va_arg(arguments, hid_t *);

                /* Retrieve the dataset's datatype */
                if((*ret_id = H5Tcopy(dset->type_id)) < 0)
                    HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get datatype ID of dataset")
                break;
            } /* end block */
        case H5VL_DATASET_GET_STORAGE_SIZE:
        case H5VL_DATASET_GET_OFFSET:
        default:
            HGOTO_ERROR(H5E_VOL, H5E_UNSUPPORTED, FAIL, "can't get this type of information from dataset")
    } /* end switch */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5VL_rados_dataset_get() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_rados_dataset_close
 *
 * Purpose:     Closes a rados HDF5 dataset.
 *
 * Return:      Success:        0
 *              Failure:        -1
 *
 * Programmer:  Neil Fortner
 *              April, 2018
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_rados_dataset_close(void *_dset, hid_t H5_ATTR_UNUSED dxpl_id,
    void H5_ATTR_UNUSED **req)
{
    H5VL_rados_dset_t *dset = (H5VL_rados_dset_t *)_dset;
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_NOAPI_NOINIT

    HDassert(dset);

    if(--dset->obj.item.rc == 0) {
        /* Free dataset data structures */
        dset->obj.oid = (char *)H5MM_xfree(dset->obj.oid);
        if(dset->type_id != FAIL && H5I_dec_app_ref(dset->type_id) < 0)
            HDONE_ERROR(H5E_DATASET, H5E_CANTDEC, FAIL, "failed to close datatype")
        if(dset->space_id != FAIL && H5I_dec_app_ref(dset->space_id) < 0)
            HDONE_ERROR(H5E_DATASET, H5E_CANTDEC, FAIL, "failed to close dataspace")
        if(dset->dcpl_id != FAIL && H5I_dec_app_ref(dset->dcpl_id) < 0)
            HDONE_ERROR(H5E_DATASET, H5E_CANTDEC, FAIL, "failed to close plist")
        if(dset->dapl_id != FAIL && H5I_dec_app_ref(dset->dapl_id) < 0)
            HDONE_ERROR(H5E_DATASET, H5E_CANTDEC, FAIL, "failed to close plist")
        dset = H5FL_FREE(H5VL_rados_dset_t, dset);
    } /* end if */

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5VL_rados_dataset_close() */
