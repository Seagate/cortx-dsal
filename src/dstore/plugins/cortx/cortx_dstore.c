/*
 * Filename:         cortx_dstore.c
 * Description:      Implementation of CORTX dstore.
 *
 * Copyright (c) 2020 Seagate Technology LLC and/or its Affiliates
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published
 * by the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 * For any questions about this software or licensing,
 * please email opensource@seagate.com or cortx-questions@seagate.com.
 */

/*
 * This file contains APIs which implement DSAL's dstore framework,
 * on top of cortx motr object APIs.
 */

#include <sys/param.h> /* DEV_BSIZE */
#include "cortx/helpers.h" /* M0 wrappers from cortx-utils */
#include "common/log.h" /* log_* */
#include "common/helpers.h" /* RC_WRAP* */
#include "dstore.h" /* import public DSTORE API definitions */
#include "../../dstore_internal.h" /* import private DSTORE API definitions */
/* TODO: assert() calls should be replaced gradually with dassert() calls. */
#include <assert.h> /* assert() */
#include "debug.h" /* dassert */
#include "lib/vec.h" /* m0bufvec and m0indexvec */
#include "operation.h"

/** Private definition of DSTORE object for M0-based backend. */
struct cortx_dstore_obj {
	struct dstore_obj base;
	struct m0_obj cobj;
};
_Static_assert((&((struct cortx_dstore_obj *) NULL)->base) == 0,
	       "The offset of of the base field should be zero.\
	       Otherwise, the direct casts (E2D, D2E) will not work.");

/* Casts DSTORE Object to CORTX Object.
 *
 * Note: Due to the lack of de-referencing, this call may be safely
 * used before pre-conditions, e.g.:
 * @code
 *   struct cortx_dstore_obj *obj = D2E_obj(in);
 *   assert(obj);
 * @endcode
 * @see ::E2D_obj
 */
static inline
struct cortx_dstore_obj *D2E_obj(struct dstore_obj *obj)
{
	return (struct cortx_dstore_obj *) obj;
}

/* Casts CORTX Object to DSTORE Object.
 * @see ::D2E_obj
 */
static inline
struct dstore_obj *E2D_obj(struct cortx_dstore_obj *obj)
{
	return (struct dstore_obj *) obj;
}


/** IO Buffers and extents for M0-based backend.
 * This object holds the information associated with an IO operation:
 *	- IO buffers - array of sizes, array of pointers, and count.
 *	- IO range (or extents) - array of sizes,
 *		array of offsets (in the object), and count.
 * This object is needed to keep M0-related information in the one place.
 * Memory mgmt: the object keeps only references borrowed from
 * the base IO operation object (dstore_io_op.)
 */
struct cortx_io_bufext {
	/** Vector of data buffers. */
	struct m0_bufvec data;
	/* Vector of extents in the target object */
	struct m0_indexvec extents;
};

/** Private definition of DSTORE io operation for M0-based backend.
 * Memory mgmt:
 * 1. It is a self-referential structure. Additional precautions should be
 *	taken care of when this structure needs to be copied or moved.
 *	Since m0 op holds the pointer to the operation context, which in our
 *	case is the cortx_io_op, this structure cannot be moved without
 *	"re-wiring" of the op_datum field inside `cop`.
 * 2. The base field of this structure holds borrowed references to the
 *	user-provided buffers. It does not own any data, and therefore
 *	it should not free any user data. However, it may own additional
 *	resources, for example a vector of sizes or offsets.
 * 3. The vec field of this structure hold copies of the references from the
 *	base field. It is needed because the requirements state that DSAL
 *	should have a common base type for operation and data vectors,
 *	however it is not possible for M0-dependent data types (bufvec
 *	and indexvec) to be "common" data types.
 *
 * Here is an example of values for a case where IO operation should be
 * executed for two IO buffers and two different offsets:
 *
 * @{verbatim}
 * +-------------------------------------------------------------------------+
 * | 0xCAFE, 0xD0AB is the addresses of two buffers allocated		     |
 * | by the user (for example, after a malloc() call).                       |
 * |                                                                         |
 * | .base.data.dbufs  => {{ 0xCAFE }, { 0xD0AB }}                           |
 * |		points to an array of user-provided buffers where            |
 * |		the array itself is allocated by DSAL.                       |
 * | .base.data.ovec => { 0, 4096 }					     |
 * |		points to an array of offsets allocated by DSAL.             |
 * | .base.data.svec => { 4096, 4096 }					     |
 * |		points to an array of sizes allocated by DSAL.               |
 * | .vec.data.ov_buf == .base.data.dbufs                                    |
 * |		points to the same location as the base field.               |
 * | .vec.extents.iv_buf == .base.data.ovec                                  |
 * |		points to the same location as the base field.               |
 * +-------------------------------------------------------------------------+
 * @{endverbatim}
 *
 * As per the current state of M0 API, "attrs" does not have any semantic
 * meaning for us (the M0 API users), so that they are kept zeroed (empty).
 *
 * Further improvements for this data type.
 *	1. Re-usage of IO operation objects. M0 operation can be re-used,
 *	therefore it would be good to make cortx_io_op to be re-usable as well.
 *	This improvement will be when the users start maintain
 *	operation lists.
 */
struct cortx_io_op {
	struct dstore_io_op base;
	struct m0_op *cop;
	struct cortx_io_bufext vec;
	struct m0_bufvec attrs;
};

_Static_assert((&((struct cortx_io_op *) NULL)->base) == 0,
	       "The offset of of the base field should be zero.\
	       Otherwise, the direct casts (E2D, D2E) will not work.");

static inline
struct cortx_io_op *D2E_op(struct dstore_io_op *op)
{
	return (struct cortx_io_op *) op;
}

/* Casts CORTX IO operation to DSTORE IO Operation
 */
static inline
struct dstore_io_op *E2D_op(struct cortx_io_op *op)
{
	return (struct dstore_io_op *) op;
}

int cortx_ds_obj_get_id(struct dstore *dstore, dstore_oid_t *oid)
{
	int rc;

	perfc_trace_inii(PFT_DS_OBJ_GET_ID, PEM_DSAL_TO_MOTR);
	rc = m0_ufid_get((struct m0_uint128 *)oid);
	perfc_trace_attr(PEA_DSTORE_RES_RC, rc);
	perfc_trace_finii(PERFC_TLS_POP_DONT_VERIFY);
	return rc;
}

int cortx_ds_obj_create(struct dstore *dstore, void *ctx,
		        dstore_oid_t *oid)
{
	int rc;
	struct m0_uint128 fid;

	perfc_trace_inii(PFT_DS_OBJ_CREATE, PEM_DSAL_TO_MOTR);

	assert(oid != NULL);
	m0_fid_copy((struct m0_uint128 *)oid, &fid);

	RC_WRAP_LABEL(rc, out, m0store_create_object, fid);

out:
	log_debug("ctx=%p fid = "U128X_F" rc=%d", ctx, U128_P(&fid), rc);
	perfc_trace_attr(PEA_DSTORE_RES_RC, rc);
	perfc_trace_finii(PERFC_TLS_POP_DONT_VERIFY);
	return rc;
}

int cortx_ds_init(struct collection_item *cfg_items)
{
	int rc;
	perfc_trace_inii(PFT_DS_INIT, PEM_DSAL_TO_MOTR);
	rc = m0init(cfg_items);
	perfc_trace_attr(PEA_DSTORE_RES_RC, rc);
	perfc_trace_finii(PERFC_TLS_POP_DONT_VERIFY);
	return rc;
}

int cortx_ds_fini(void)
{
	perfc_trace_inii(PFT_DS_FINISH, PEM_DSAL_TO_MOTR);
	m0fini();
	perfc_trace_finii(PERFC_TLS_POP_DONT_VERIFY);
	return 0;
}

int cortx_ds_obj_del(struct dstore *dstore, void *ctx, dstore_oid_t *oid)
{
	struct m0_uint128 fid;
	int rc;

	perfc_trace_inii(PFT_DS_OBJ_DELETE, PEM_DSAL_TO_MOTR);

	assert(oid != NULL);

	m0_fid_copy((struct m0_uint128 *)oid, &fid);

	/* Delete the object from backend store */
	rc = m0store_delete_object(fid);

	if (rc) {
		if (rc == -ENOENT) {
			log_warn("Non-existing obj, ctx=%p fid= "U128X_F" rc=%d",
				  ctx, U128_P(&fid), rc);
			goto out;
		}
		log_err("Unable to delete object, ctx=%p fid= "U128X_F" rc=%d",
			 ctx, U128_P(&fid), rc);
		goto out;
	}

out:
	log_debug("EXIT: ctx=%p fid= "U128X_F" rc=%d", ctx, U128_P(&fid), rc);
	perfc_trace_attr(PEA_DSTORE_RES_RC, rc);
	perfc_trace_finii(PERFC_TLS_POP_DONT_VERIFY);

	return rc;
}

static int cortx_dstore_obj_alloc(struct cortx_dstore_obj **out)
{
	int rc = 0;
	struct cortx_dstore_obj *obj;

	perfc_trace_inii(PFT_DS_OBJ_ALLOC, PEM_DSAL_TO_MOTR);

	perfc_trace_attr(PEA_TIME_ATTR_START_M0_ALLOC_PTR);
	M0_ALLOC_PTR(obj);
	perfc_trace_attr(PEA_TIME_ATTR_END_M0_ALLOC_PTR);

	if (!obj) {
		rc = RC_WRAP_SET(-ENOMEM);
		goto out;
	}

	*out = obj;

out:
	perfc_trace_attr(PEA_DSTORE_RES_RC, rc);
	perfc_trace_finii(PERFC_TLS_POP_DONT_VERIFY);

	return rc;
}

static void cortx_dstore_obj_free(struct cortx_dstore_obj *obj)
{
	perfc_trace_inii(PFT_DS_OBJ_FREE, PEM_DSAL_TO_MOTR);
	perfc_trace_attr(PEA_TIME_ATTR_START_M0_FREE);
	m0_free(obj);
	perfc_trace_attr(PEA_TIME_ATTR_END_M0_FREE);
	perfc_trace_finii(PERFC_TLS_POP_DONT_VERIFY);
}

static int cortx_ds_obj_open(struct dstore *dstore, const obj_id_t *oid,
			     struct dstore_obj **out)
{
	int rc;
	struct cortx_dstore_obj *obj = NULL;

	perfc_trace_inii(PFT_DS_OBJ_OPEN, PEM_DSAL_TO_MOTR);

	RC_WRAP_LABEL(rc, out, cortx_dstore_obj_alloc, &obj);

	RC_WRAP_LABEL(rc, out, m0store_obj_open, oid, &obj->cobj);

	*out = E2D_obj(obj);
	obj = NULL;

out:
	cortx_dstore_obj_free(obj);
	perfc_trace_attr(PEA_DSTORE_RES_RC, rc);
	perfc_trace_finii(PERFC_TLS_POP_DONT_VERIFY);
	return rc;
}

static int cortx_ds_obj_close(struct dstore_obj *dobj)
{
	struct cortx_dstore_obj *obj = D2E_obj(dobj);

	perfc_trace_inii(PFT_DS_OBJ_CLOSE, PEM_DSAL_TO_MOTR);

	dassert(obj);
	dassert(obj->base.ds);

	m0store_obj_close(&obj->cobj);

	cortx_dstore_obj_free(obj);

	perfc_trace_finii(PERFC_TLS_POP_DONT_VERIFY);
	/* XXX:
	 * Right now, we assume that M0-based backend
	 * cannot fail here. However, later on
	 * we may want to return an error if we have unfinished
	 * M0 operations. Such operations (that have not been
	 * properly finished) may cause failures, for instance,
	 * during finalization of DSAL module.
	 */
	return 0;
}

/* NOTE: The function may be unsafe because we have two references to
 * the same object (svec) from two different objects (data and extents).
 * However, it is safe as long as M0 is not trying to modify both of them
 * as independent objects (for example, using memcpy or something similar).
 * Since logically M0 should not mutate these fields (READ,WRITE,ALLOC,FREE
 * do not modify the vectors except the contents of data buffers), it is
 * safe to do this assignment here.
 */
static inline
void dstore_io_vec2bufext(struct dstore_io_vec *io_vec,
			  struct cortx_io_bufext *bufext)
{
	M0_SET0(bufext);
	bufext->data.ov_buf = (void **) io_vec->dbufs;
	bufext->data.ov_vec.v_nr = io_vec->nr;
	bufext->data.ov_vec.v_count = io_vec->svec;

	bufext->extents.iv_vec.v_nr = io_vec->nr;
	bufext->extents.iv_vec.v_count = io_vec->svec;
	bufext->extents.iv_index = io_vec->ovec;
}

static void on_oop_executed(struct m0_op *cop)
{
	log_trace("IO op %p executed.", cop->op_datum);
	/* noop */
}


static void on_oop_finished(struct m0_op *cop)
{
	int rc = m0_rc(cop);
	struct cortx_io_op *op = cop->op_datum;
	dassert(op->cop == cop);
	RC_WRAP_SET(rc);
	if (op->base.cb) {
		op->base.cb(op->base.cb_ctx, &op->base, rc);
	}
	log_trace("IO op %p finished.", op);
}

static void on_oop_failed(struct m0_op *cop)
{
	log_trace("IO op %p went to failed state.", cop->op_datum);
	on_oop_finished(cop);
}

static const struct m0_op_ops cortx_io_op_cbs = {
	.oop_executed = on_oop_executed,
	.oop_failed = on_oop_failed,
	.oop_stable = on_oop_finished,
};
/* Return an appropriate object operation value with respect to io operation
 * value
 */
static enum m0_obj_opcode
	dstore_io_op_type2m0_op_type(enum dstore_io_op_type type)
{
	enum m0_obj_opcode obj_opcode;
	switch (type) {
		case DSTORE_IO_OP_WRITE:
			obj_opcode = M0_OC_WRITE;
			break;
		case DSTORE_IO_OP_READ:
			obj_opcode = M0_OC_READ;
			break;
		case DSTORE_IO_OP_FREE:
			obj_opcode = M0_OC_FREE;
			break;
		default:
			/* Unsupported operation type */
			dassert(0);
	}
	return obj_opcode;
}

static int cortx_ds_io_op_init(struct dstore_obj *dobj,
			       enum dstore_io_op_type type,
			       struct dstore_io_vec *bvec,
			       dstore_io_op_cb_t cb,
			       void *cb_ctx,
			       struct dstore_io_op **out)
{
	int rc = 0;
	struct cortx_dstore_obj *obj = D2E_obj(dobj);
	struct cortx_io_op *result = NULL;

	const m0_time_t schedule_now = 0;
	const uint64_t empty_mask = 0;
	const uint64_t empty_flag = 0;

	perfc_trace_inii(PFT_DS_IO_INIT, PEM_DSAL_TO_MOTR);

	if (!M0_IN(type, (DSTORE_IO_OP_WRITE, DSTORE_IO_OP_READ, DSTORE_IO_OP_FREE))) {
		log_err("%s", (char *) "Unsupported IO operation");
		rc = RC_WRAP_SET(-EINVAL);
		goto out;
	}

	dassert(bvec);
	dassert(out);
	dassert(dstore_io_vec_invariant(bvec));

	M0_ALLOC_PTR(result);
	if (result == NULL) {
		rc = RC_WRAP_SET(-ENOMEM);
		goto out;
	}

	result->base.type = type;
	result->base.obj = dobj;
	result->base.cb = cb;
	result->base.cb_ctx = cb_ctx;

	if (dstore_io_vec_flags_has_data(bvec->flags)) {
		/* READ/WRITE Operation */
		dstore_io_vec_move(&result->base.data, bvec);
		dstore_io_vec2bufext(&result->base.data, &result->vec);
		perfc_trace_attr(PEA_TIME_ATTR_START_M0_OBJ_OP);
		RC_WRAP_LABEL(rc, out, m0_obj_op, &obj->cobj,
			      dstore_io_op_type2m0_op_type(type), &result->vec.extents,
			      &result->vec.data,
			      &result->attrs, empty_mask, empty_flag, &result->cop);
	}
	else {
		/* Free operation */
		result->vec.extents.iv_vec.v_nr = bvec->nr;
		result->vec.extents.iv_vec.v_count = bvec->svec;
		result->vec.extents.iv_index = bvec->ovec;
		perfc_trace_attr(PEA_TIME_ATTR_START_M0_OBJ_OP);
		RC_WRAP_LABEL(rc, out, m0_obj_op, &obj->cobj,
			      dstore_io_op_type2m0_op_type(type), &result->vec.extents,
			      NULL, NULL,
			      empty_mask, empty_flag, &result->cop);
	}

	perfc_trace_attr(PEA_TIME_ATTR_END_M0_OBJ_OP);
	perfc_trace_attr(PEA_M0_OP_SM_ID, result->cop->op_sm.sm_id);
	perfc_trace_attr(PEA_M0_OP_SM_STATE, result->cop->op_sm.sm_state);

	result->cop->op_datum = result;
	m0_op_setup(result->cop, &cortx_io_op_cbs, schedule_now);

	*out = E2D_op(result);
	result = NULL;

out:
	if (result) {
		m0_free(result);
	}

	log_debug("io_op_init obj=%p, nr=%d, op=%p rc=%d", obj, (int) bvec->nr,
		  rc == 0 ? *out : NULL, rc);

	perfc_trace_attr(PEA_DSTORE_RES_RC, rc);
	perfc_trace_finii(PERFC_TLS_POP_DONT_VERIFY);

	dassert((!(*out)) || dstore_io_op_invariant(*out));
	return rc;
}

static int cortx_ds_io_op_submit(struct dstore_io_op *dop)
{
	struct cortx_io_op *op = D2E_op(dop);
	perfc_trace_inii(PFT_DS_IO_SUBMIT, PEM_DSAL_TO_MOTR);
	perfc_trace_attr(PEA_TIME_ATTR_START_M0_OP_LAUNCH);

	m0_op_launch(&op->cop, 1);

	perfc_trace_attr(PEA_TIME_ATTR_END_M0_OP_LAUNCH);
	perfc_trace_attr(PEA_M0_OP_SM_ID, op->cop->op_sm.sm_id);
	perfc_trace_attr(PEA_M0_OP_SM_STATE, op->cop->op_sm.sm_state);

	log_debug("io_op_submit op=%p", op);
	perfc_trace_finii(PERFC_TLS_POP_DONT_VERIFY);
	return 0; /* M0 launch is safe */
}

static int cortx_ds_io_op_wait(struct dstore_io_op *dop)
{
	int rc;
	struct cortx_io_op *op = D2E_op(dop);
	const uint64_t wait_bits = M0_BITS(M0_OS_FAILED,
					   M0_OS_STABLE);
	const m0_time_t time_limit = M0_TIME_NEVER;

	perfc_trace_inii(PFT_DS_IO_WAIT, PEM_DSAL_TO_MOTR);

	perfc_trace_attr(PEA_TIME_ATTR_START_M0_OP_WAIT);
	RC_WRAP_LABEL(rc, out, m0_op_wait, op->cop, wait_bits,
		      time_limit);
	perfc_trace_attr(PEA_TIME_ATTR_END_M0_OP_WAIT);

	perfc_trace_attr(PEA_TIME_ATTR_START_M0_RC);
	RC_WRAP_LABEL(rc, out, m0_rc, op->cop);
	perfc_trace_attr(PEA_TIME_ATTR_END_M0_RC);

	perfc_trace_attr(PEA_M0_OP_SM_ID, op->cop->op_sm.sm_id);
	perfc_trace_attr(PEA_M0_OP_SM_STATE, op->cop->op_sm.sm_state);

out:
	log_debug("io_op_wait op=%p, rc=%d", op, rc);
	perfc_trace_attr(PEA_DSTORE_RES_RC, rc);
	perfc_trace_finii(PERFC_TLS_POP_DONT_VERIFY);

	return rc;
}

static void cortx_ds_io_op_fini(struct dstore_io_op *dop)
{
	struct cortx_io_op *op = D2E_op(dop);

	perfc_trace_inii(PFT_DS_IO_FINISH, PEM_DSAL_TO_MOTR);

	perfc_trace_attr(PEA_M0_OP_SM_ID, op->cop->op_sm.sm_id);
	perfc_trace_attr(PEA_M0_OP_SM_STATE, op->cop->op_sm.sm_state);

	perfc_trace_attr(PEA_TIME_ATTR_START_M0_OP_FINISH);
	m0_op_fini(op->cop);
	perfc_trace_attr(PEA_TIME_ATTR_END_M0_OP_FINISH);

	perfc_trace_attr(PEA_TIME_ATTR_START_M0_OP_FREE);
	m0_op_free(op->cop);
	perfc_trace_attr(PEA_TIME_ATTR_END_M0_OP_FREE);

	perfc_trace_attr(PEA_TIME_ATTR_START_M0_FREE);
	m0_free(op);
	perfc_trace_attr(PEA_TIME_ATTR_END_M0_FREE);

	perfc_trace_finii(PERFC_TLS_POP_DONT_VERIFY);
}

ssize_t cortx_ds_obj_get_bsize(dstore_oid_t *oid)
{
	ssize_t bsize;
	struct m0_uint128 fid;
	m0_fid_copy((struct m0_uint128 *)oid, &fid);
	bsize = m0store_get_bsize(fid);
	log_debug("cortx_ds_obj_get_bsize bsize %lu", bsize);
	return bsize;
}

const struct dstore_ops cortx_dstore_ops = {
	.init = cortx_ds_init,
	.fini = cortx_ds_fini,
	.obj_create = cortx_ds_obj_create,
	.obj_delete = cortx_ds_obj_del,
	.obj_get_id = cortx_ds_obj_get_id,
	.obj_open = cortx_ds_obj_open,
	.obj_close = cortx_ds_obj_close,
	.io_op_init = cortx_ds_io_op_init,
	.io_op_submit = cortx_ds_io_op_submit,
	.io_op_wait = cortx_ds_io_op_wait,
	.io_op_fini = cortx_ds_io_op_fini,
	.obj_get_bsize = cortx_ds_obj_get_bsize,
};
