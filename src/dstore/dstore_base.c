/*
 * Filename:         dstore_base.c
 * Description:      Contains implementation of basic dstore
 *                   framework APIs.
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

#include <stdlib.h>
#include "dstore.h"
#include <assert.h> /* TODO: to be replaced with dassert() */
#include <errno.h> /* ret codes such as EINVAL */
#include <string.h> /* strncmp, strlen */
#include <ini_config.h> /* collection_item and related functions */
#include "common/helpers.h" /* RC_WRAP* */
#include "common/log.h" /* log_* */
#include "debug.h" /* dassert */
#include "dstore_internal.h" /* import internal API definitions */
#include "dstore_bufvec.h" /* data buffers and vectors */
#include "operation.h"

static struct dstore g_dstore;

struct dstore *dstore_get(void)
{
	return &g_dstore;
}

struct dstore_module {
	char *type;
	const struct dstore_ops *ops;
};

static struct dstore_module dstore_modules[] = {
	{ "cortx", &cortx_dstore_ops },
	{ NULL, NULL },
};

int dstore_init(struct collection_item *cfg, int flags)
{
	int rc;
	struct dstore *dstore = dstore_get();
	struct collection_item *item = NULL;
	const struct dstore_ops *dstore_ops = NULL;
	char *dstore_type = NULL;
	int i;

	assert(dstore && cfg);

	RC_WRAP(get_config_item, "dstore", "type", cfg, &item);
	if (item == NULL) {
		fprintf(stderr, "dstore type not specified\n");
		return -EINVAL;
	}

	dstore_type = get_string_config_value(item, NULL);

	assert(dstore_type != NULL);

	for (i = 0; dstore_modules[i].type != NULL; ++i) {
		if (strncmp(dstore_type, dstore_modules[i].type,
		    strlen(dstore_type)) == 0) {
			dstore_ops = dstore_modules[i].ops;
			break;
		}
	}

	dstore->type = dstore_type;
	dstore->cfg = cfg;
	dstore->flags = flags;
	dstore->dstore_ops = dstore_ops;
	assert(dstore->dstore_ops != NULL);

	assert(dstore->dstore_ops->init != NULL);
	rc = dstore->dstore_ops->init(cfg);
	if (rc) {
		return rc;
	}

	return 0;
}

int dstore_fini(struct dstore *dstore)
{
	assert(dstore && dstore->dstore_ops && dstore->dstore_ops->fini);

	return dstore->dstore_ops->fini();
}

int dstore_obj_create(struct dstore *dstore, void *ctx,
		      dstore_oid_t *oid)
{
	assert(dstore && dstore->dstore_ops && oid &&
	       dstore->dstore_ops->obj_create);

	return dstore->dstore_ops->obj_create(dstore, ctx, oid);
}

int dstore_obj_delete(struct dstore *dstore, void *ctx,
		      dstore_oid_t *oid)
{
	assert(dstore && dstore->dstore_ops && oid &&
	       dstore->dstore_ops->obj_delete);

	return dstore->dstore_ops->obj_delete(dstore, ctx, oid);
}

/* TODO: Can be removed when plugin API for removing objects from backend store
 * is implemented
 */
#define DSAL_MAX_IO_SIZE (1024*1024)

static int dstore_obj_shrink(struct dstore_obj *obj,  size_t old_size,
			     size_t new_size)
{
	int rc;
	size_t nr_request;
	size_t tail_size;
	size_t index;
	size_t bsize;
	size_t count;
	off_t offset;
	char *tmp_buf = NULL;

	bsize = dstore_get_bsize(obj->ds,
				 (dstore_oid_t *)dstore_obj_id(obj));

	count = old_size - new_size;
	offset = new_size;

	/* Temporary space to have all zeroed out data to be written in to a
	 * given object for specified range
	 * At any given point of time we won't be writing more than
	 * DSAL_MAX_IO_SIZE
	 */
	tmp_buf = calloc(1, DSAL_MAX_IO_SIZE);

	if (tmp_buf == NULL ) {
		rc = -ENOMEM;
		log_err("dstore_obj_resize: Could not allocate memory");
		goto out;
	}

	/* TODO: Below logic is a temporary workaround to make sure after shrink
	 * operation if we do extend then user will get all zeroes instead of
	 * getting old/stale data, this logic can be deprecated once dstore
	 * plugin API to remove truncated object is implemented, which can be
	 * called directly from here.
	 */

	nr_request =  count / DSAL_MAX_IO_SIZE;
	tail_size = count - (nr_request * DSAL_MAX_IO_SIZE);

	for (index = 0; index < nr_request; index++) {
		RC_WRAP_LABEL(rc, out, dstore_pwrite, obj,
			      offset + (index * DSAL_MAX_IO_SIZE),
			      DSAL_MAX_IO_SIZE, bsize, tmp_buf);
	}

	if (tail_size) {
		/* Write down remaining data */
		RC_WRAP_LABEL(rc, out, dstore_pwrite, obj,
			      offset + (index * DSAL_MAX_IO_SIZE), tail_size,
			      bsize, tmp_buf);
	}
out:
	if (tmp_buf != NULL ) {
		free(tmp_buf);
	}

	log_trace("dstore_obj_shrink:(" OBJ_ID_F " <=> %p )"
		  "old_size = %lu new_size = %lu rc = %d",
		  OBJ_ID_P(dstore_obj_id(obj)), obj, old_size, new_size,
		  rc);
	return rc;
}

int dstore_obj_resize(struct dstore_obj *obj, size_t old_size, size_t new_size)
{
	int rc = 0;

	/* Following code handle two cases
	 * 1. If old and new size are same it's a noop hence no change
	 * 2. If old < new size, the extra ranges is considered as a hole and
	 * while reading back those user will get all zero for this range
	 */
	if (old_size <= new_size) {
		log_trace("dstore_obj_resize:(" OBJ_ID_F " <=> %p )"
			  "old_size = %lu new_size = %lu rc = %d",
			  OBJ_ID_P(dstore_obj_id(obj)), obj, old_size, new_size,
			  rc);
		goto out;
	}

	/* Shrink operation */
	RC_WRAP_LABEL(rc, out, dstore_obj_shrink, obj, old_size, new_size);
out:
	log_trace("dstore_obj_resize:(" OBJ_ID_F " <=> %p )"
		  "old_size = %lu new_size = %lu rc = %d",
		  OBJ_ID_P(dstore_obj_id(obj)), obj, old_size, new_size,
		  rc);
	return rc;
}

int dstore_get_new_objid(struct dstore *dstore, dstore_oid_t *oid)
{
	assert(dstore && oid && dstore->dstore_ops &&
	       dstore->dstore_ops->obj_get_id);

	return dstore->dstore_ops->obj_get_id(dstore, oid);
}

int dstore_obj_open(struct dstore *dstore,
		    const dstore_oid_t *oid,
		    struct dstore_obj **out)
{
	int rc;
	struct dstore_obj *result = NULL;

	dassert(dstore);
	dassert(oid);
	dassert(out);

	RC_WRAP_LABEL(rc, out, dstore->dstore_ops->obj_open, dstore, oid,
		      &result);

	result->ds = dstore;
	result->oid = *oid;

	/* Transfer the ownership of the created object to the caller. */
	*out = result;
	result = NULL;

out:
	if (result) {
		dstore_obj_close(result);
	}

	log_debug("open " OBJ_ID_F ", %p, rc=%d", OBJ_ID_P(oid),
		  rc == 0 ? *out : NULL, rc);
	return rc;
}

int dstore_obj_close(struct dstore_obj *obj)
{
	int rc;
	struct dstore *dstore;

	dassert(obj);
	dstore = obj->ds;
	dassert(dstore);

	log_trace("close >>> " OBJ_ID_F ", %p",
		  OBJ_ID_P(dstore_obj_id(obj)), obj);

	RC_WRAP_LABEL(rc, out, dstore->dstore_ops->obj_close, obj);

out:
	log_trace("close <<< (%d)", rc);
	return rc;
}

static int dstore_io_op_init_and_submit(struct dstore_obj *obj,
                                        struct dstore_io_vec *bvec,
                                        struct dstore_io_op **out,
                                        enum dstore_io_op_type op_type)
{
	int rc;
	struct dstore *dstore;
	struct dstore_io_op *result = NULL;

	dassert(obj);
	dassert(obj->ds);
	dassert(bvec);
	dassert(out);
	dassert(dstore_obj_invariant(obj));
	dassert(dstore_io_vec_invariant(bvec));
	/* Only WRITE/READ is supported so far */
	dassert(op_type == DSTORE_IO_OP_WRITE ||
		op_type == DSTORE_IO_OP_READ);

	dstore = obj->ds;

	RC_WRAP_LABEL(rc, out, dstore->dstore_ops->io_op_init, obj,
		      op_type, bvec, NULL, NULL, &result);
	RC_WRAP_LABEL(rc, out, dstore->dstore_ops->io_op_submit, result);

	*out = result;
	result = NULL;

out:
	if (result) {
		dstore->dstore_ops->io_op_fini(result);
	}

	dassert((!(*out)) || dstore_io_op_invariant(*out));
	return rc;
}

int dstore_io_op_write(struct dstore_obj *obj,
                       struct dstore_io_vec *bvec,
                       struct dstore_io_op **out)
{
	int rc;

	rc = dstore_io_op_init_and_submit(obj, bvec, out, DSTORE_IO_OP_WRITE);

	log_debug("write (" OBJ_ID_F " <=> %p, "
		  "vec=%p, *out=%p) rc=%d",
		  OBJ_ID_P(dstore_obj_id(obj)), obj,
		  bvec, rc == 0 ? *out : NULL, rc);

	return rc;
}

int dstore_io_op_read(struct dstore_obj *obj, struct dstore_io_vec *bvec,
                      struct dstore_io_op **out)
{
	int rc;

	rc = dstore_io_op_init_and_submit(obj, bvec, out, DSTORE_IO_OP_READ);

	log_debug("read (" OBJ_ID_F " <=> %p, "
		  "vec=%p, *out=%p) rc=%d",
		  OBJ_ID_P(dstore_obj_id(obj)), obj,
		  bvec, rc == 0 ? *out : NULL, rc);

	return rc;
}

int dstore_io_op_wait(struct dstore_io_op *op)
{
	int rc = 0;
	struct dstore *dstore;

	dassert(op);
	dassert(op->obj);
	dassert(op->obj->ds);
	dassert(dstore_io_op_invariant(op));

	dstore = op->obj->ds;

	RC_WRAP_LABEL(rc, out, dstore->dstore_ops->io_op_wait, op);

out:
	log_debug("wait (" OBJ_ID_F " <=> %p, op=%p) rc=%d",
		  OBJ_ID_P(dstore_obj_id(op->obj)), op->obj, op, rc);
	return rc;
}

void dstore_io_op_fini(struct dstore_io_op *op)
{
	struct dstore *dstore;

	dassert(op);
	dassert(op->obj);
	dassert(op->obj->ds);
	dassert(dstore_io_op_invariant(op));

	dstore = op->obj->ds;

	log_trace("fini >>> (" OBJ_ID_F " <=> %p, op=%p)",
		  OBJ_ID_P(dstore_obj_id(op->obj)), op->obj, op);

	dstore->dstore_ops->io_op_fini(op);

	log_trace("%s", (char *) "fini <<< ()");
}

static ssize_t __dstore_get_bsize(struct dstore *dstore, dstore_oid_t *oid)
{
	dassert(dstore && oid);
	dassert(dstore_invariant(dstore));

	return dstore->dstore_ops->obj_get_bsize(oid);
}


ssize_t dstore_get_bsize(struct dstore *dstore, dstore_oid_t *oid)
{
	size_t rc;

	perfc_trace_inii(PFT_DSTORE_GET, PEM_DSTORE_TO_NFS);

	rc = __dstore_get_bsize(dstore, oid);

	perfc_trace_attr(PEA_DSTORE_GET_RES_RC, rc);
	perfc_trace_finii(PERFC_TLS_POP_DONT_VERIFY);

	return rc;
}

static int pwrite_aligned(struct dstore_obj *obj, char *write_buf,
			  size_t buf_size, off_t offset)
{
	int rc = 0;

	dassert(obj);
        dassert(write_buf);
        dassert(offset >= 0);

        struct dstore_io_op *wop = NULL;
        struct dstore_io_vec *data = NULL;
        struct dstore_io_buf *buf = NULL;

	RC_WRAP_LABEL(rc, out, dstore_io_buf_init, write_buf, buf_size,
		      offset, &buf);

	RC_WRAP_LABEL(rc, out, dstore_io_buf2vec, &buf, &data);

	RC_WRAP_LABEL(rc, out, dstore_io_op_write, obj, data, &wop);

	RC_WRAP_LABEL(rc, out, dstore_io_op_wait, wop);

out:
	if (wop) {
		dstore_io_op_fini(wop);
	}

	if (data) {
		dstore_io_vec_fini(data);
	}

	if (buf) {
		dstore_io_buf_fini(buf);
	}

	log_trace("pwrite_aligned:(" OBJ_ID_F " <=> %p ) offset = %lu"
		  "size = %lu rc = %d",
		  OBJ_ID_P(dstore_obj_id(obj)), obj, offset, buf_size, rc);

	return rc;
}

static int pread_aligned(struct dstore_obj *obj, char *read_buf,
			 size_t buf_size, off_t offset)
{
	int rc = 0;

	dassert(obj);
	dassert(read_buf);
	dassert(offset >= 0);

        struct dstore_io_op *rop = NULL;
        struct dstore_io_vec *data = NULL;
        struct dstore_io_buf *buf = NULL;

        RC_WRAP_LABEL(rc, out, dstore_io_buf_init, read_buf, buf_size,
                      offset, &buf);

        RC_WRAP_LABEL(rc, out, dstore_io_buf2vec, &buf, &data);

        RC_WRAP_LABEL(rc, out, dstore_io_op_read, obj, data, &rop);

        RC_WRAP_LABEL(rc, out, dstore_io_op_wait, rop);

out:
        if (rop) {
                dstore_io_op_fini(rop);
        }

        if (data) {
                dstore_io_vec_fini(data);
        }

        if (buf) {
                dstore_io_buf_fini(buf);
        }

	log_trace("pread_aligned:(" OBJ_ID_F " <=> %p ) offset = %lu"
		  "size = %lu rc = %d",
		  OBJ_ID_P(dstore_obj_id(obj)), obj, offset, buf_size, rc);

        return rc;
}

static
int pread_aligned_handle_holes(struct dstore_obj *obj, char *read_buf,
			       size_t buf_size, off_t offset, size_t bs)
{
	int rc = 0;

	rc = pread_aligned(obj, read_buf, buf_size, offset);

	/* The following logic handles two case which are explained below
	 * 1. Motr is not able to handle the case where some part of object
	 * has not been written or created. For that it returns -ENOENT
	 * even though some of them are available and we should get valid data
	 * for them atleast. For such case, this is the workaround where
	 * if we are reading more than one block size we will read
	 * all the block one by one so that for originally available
	 * block we will get proper data.
	 * 2. In case of sparse block, the block which is not written will be
	 * filled with all zeros.
	*/
	if (rc == -ENOENT)
	{
		int count = buf_size/bs;
		int i;

		for (i = 0; i < count; i++)
		{
			/* read block one by one */
			rc = pread_aligned(obj, read_buf + (i*bs), bs,
					   offset + (i * bs));

			if (rc != 0)
			{
				if (rc == -ENOENT)
				{
					memset(read_buf + (i * bs), 0, bs);
				}
				else
				{
					log_err("Unable to read a block at"
						"offset %lu block size %lu"
						"(" OBJ_ID_F " <=> %p ) rc %d",
						offset + (i * bs), bs,
						OBJ_ID_P(dstore_obj_id(obj)),
						obj, rc);
					return rc;
				}
			}
		}

		rc = 0;
	}

	log_trace("pread_aligned_handle_holes:(" OBJ_ID_F " <=> %p )"
		  "offset = %lu size = %lu rc = %d",
		  OBJ_ID_P(dstore_obj_id(obj)), obj, offset, buf_size, rc);

	return rc;
}

static int pwrite_unaligned(struct dstore_obj *obj, off_t offset, size_t count,
			    size_t bs, char *buf)
{
	int rc = 0;

	uint32_t left_blk_num = offset/bs;

	uint32_t right_blk_num = (offset + count)/bs;
	if ((offset + count) % bs == 0)
		right_blk_num--;

	uint32_t num_of_blks = right_blk_num - left_blk_num + 1;

	char *tmpbuf = calloc(num_of_blks*bs, sizeof(char));

	if (tmpbuf == NULL)
	{
		rc = -ENOMEM;
		log_err("Could not allocate memory");
		goto out;
	}

	/* IO is not already left aligned, read left most block */
	if ((offset % bs) != 0)
	{
		rc = pread_aligned_handle_holes(obj, tmpbuf,
						bs, left_blk_num*bs,
						bs);
		if (rc < 0)
		{
			log_err("Read failed at offset %lu block size %lu,"
				"(" OBJ_ID_F " <=> %p ) rc %d",
				left_blk_num * bs, bs,
				OBJ_ID_P(dstore_obj_id(obj)), obj, rc);
			goto out;
		}
	}

	/* IO is not already right aligned, read right most block */
	if ((offset + count) % bs != 0 && left_blk_num != right_blk_num)
	{
		rc = pread_aligned_handle_holes(obj,
						(tmpbuf +
						 ((num_of_blks - 1) * bs)),
						bs, right_blk_num * bs, bs);
		if (rc < 0)
		{
			log_err("Read failed at offset %lu block size %lu,"
				"(" OBJ_ID_F " <=> %p ) rc %d",
				right_blk_num * bs, bs,
				OBJ_ID_P(dstore_obj_id(obj)), obj, rc);
			goto out;
		}
	}

	uint32_t buf_pos = offset - (left_blk_num * bs);
	memcpy(tmpbuf + buf_pos, buf, count);

	/* Do one write which is both left and right aligned */
	rc = pwrite_aligned(obj, tmpbuf, num_of_blks * bs,
			    left_blk_num * bs);

	if (rc < 0)
	{
		log_err("Write failed at offset %lu block size %lu,"
			"(" OBJ_ID_F " <=> %p ) rc %d", left_blk_num * bs, bs,
			OBJ_ID_P(dstore_obj_id(obj)), obj, rc);
		goto out;
	}
out:

	if (tmpbuf)
	{
		free(tmpbuf);
	}

	log_trace("pwrite_unaligned:(" OBJ_ID_F " <=> %p )"
		  "offset = %lu size = %lu rc = %d",
		  OBJ_ID_P(dstore_obj_id(obj)), obj, offset, count, rc);
	return rc;
}


static int pread_unaligned(struct dstore_obj *obj, off_t offset, size_t count,
			   size_t bs, char *buf)
{
	int rc = 0;
	uint32_t cont_blk_count = 0;
	uint32_t buf_pos = 0;
	uint32_t left_blk_num = 0;
	uint32_t left_bytes = 0;
	uint32_t right_bytes = 0;
	uint32_t read_count = 0;

	char *tmpbuf = calloc(bs, sizeof(char));
	if (tmpbuf == NULL)
	{
		rc = -ENOMEM;
		log_err("Could not allocate memory");
		goto out;
	}

	if (offset % bs == 0 && count >= bs)
	{
		/* IO is already left aligned */
		goto continous_aligned_read;
	}

	left_blk_num = offset/bs;
	left_bytes = offset - (left_blk_num * bs);
	right_bytes = bs - left_bytes;

	/* check for an insider block */
	/* for if we have to read only 100 bytes from a block,
	 * this block is insider block */
	read_count = (count < right_bytes) ? count : right_bytes;

	/* read left most block */
	rc = pread_aligned_handle_holes(obj, tmpbuf, bs,
					left_blk_num * bs, bs);

	if (rc < 0)
	{
		log_err("Read failed at offset %lu block size %lu"
			"(" OBJ_ID_F " <=> %p ) rc %d",
			left_blk_num * bs, bs,
			OBJ_ID_P(dstore_obj_id(obj)), obj, rc);
		goto out;
	}

	memcpy(buf, tmpbuf + left_bytes, read_count);

	if (count <= right_bytes)
	{
		goto out;
	}

	count = count - read_count;
	offset = offset + read_count;
	buf_pos = read_count;

continous_aligned_read:

	cont_blk_count = count/bs;

	if (cont_blk_count > 0)
	{
		rc = pread_aligned_handle_holes(obj, buf + buf_pos,
						cont_blk_count * bs, offset,
						bs);
		if (rc < 0)
		{
			log_err("Read failed at offset %lu block size %lu,"
				"(" OBJ_ID_F " <=> %p ) rc %d", offset, bs,
				OBJ_ID_P(dstore_obj_id(obj)), obj, rc);
			goto out;
		}

		count = count - (cont_blk_count * bs);
		offset = offset + (cont_blk_count * bs);
		buf_pos = buf_pos + (cont_blk_count * bs);
	}

	dassert (count >= 0);

	if (count == 0) /* Io is already right aligned */
		goto out;

	/* read the right most block */
	rc = pread_aligned_handle_holes(obj, tmpbuf, bs,
					offset, bs);
	if (rc < 0)
	{
		log_err("Read failed at offset %lu block size %lu,"
			"(" OBJ_ID_F " <=> %p ) rc %d", offset, bs,
			OBJ_ID_P(dstore_obj_id(obj)), obj, rc);
		goto out;
	}

	memcpy(buf + buf_pos, tmpbuf, count);

out:
	if (tmpbuf)
		free(tmpbuf);

	log_trace("pread_unaligned:(" OBJ_ID_F " <=> %p )"
		  "offset = %lu size = %lu rc = %d",
		  OBJ_ID_P(dstore_obj_id(obj)), obj, offset, count, rc);
	return rc;
}

static int __dstore_pwrite(struct dstore_obj *obj, off_t offset, size_t count,
		  size_t bs, char *buf)
{
	int rc = 0;

	dassert(obj);
	dassert(buf);

	if (count % bs == 0 && offset % bs == 0)
	{
		rc = pwrite_aligned(obj, buf, count, offset);
	}
	else
	{
		rc = pwrite_unaligned(obj, offset, count, bs, buf);
	}

	log_trace("dstore_pwrite:(" OBJ_ID_F " <=> %p )"
		  "offset = %lu size = %lu rc = %d",
		  OBJ_ID_P(dstore_obj_id(obj)), obj, offset, count, rc);
	return rc;
}

int dstore_pwrite(struct dstore_obj *obj, off_t offset, size_t count,
		 size_t bs, char *buf)
{
	int rc;

	perfc_trace_inii(PFT_DSTORE_PWRITE, PEM_DSTORE_TO_NFS);
	perfc_trace_attr(PEA_DSTORE_PWRITE_OFFSET, offset);
	perfc_trace_attr(PEA_DSTORE_PWRITE_COUNT, count);
	perfc_trace_attr(PEA_DSTORE_PWRITE_BS, bs);

	rc = __dstore_pwrite(obj, offset, count, bs, buf);

	perfc_trace_attr(PEA_DSTORE_PWRITE_RES_RC, rc);
	perfc_trace_finii(PERFC_TLS_POP_DONT_VERIFY);

	return rc;
}

static int __dstore_pread(struct dstore_obj *obj, off_t offset, size_t count,
			  size_t bs, char *buf)
{
	int rc = 0;

	dassert(obj);
	dassert(buf);

	if (count % bs == 0 && offset % bs == 0)
	{
		rc = pread_aligned_handle_holes(obj, buf, count, offset, bs);
	}
	else
	{
		rc = pread_unaligned(obj, offset, count, bs, buf);
	}

	log_trace("dstore_pread:(" OBJ_ID_F " <=> %p )"
		  "offset = %lu size = %lu rc = %d",
		  OBJ_ID_P(dstore_obj_id(obj)), obj, offset, count, rc);
	return rc;
}

int dstore_pread(struct dstore_obj *obj, off_t offset, size_t count,
		 size_t bs, char *buf)
{
	int rc;

	perfc_trace_inii(PFT_DSTORE_PREAD, PEM_DSTORE_TO_NFS);
	perfc_trace_attr(PEA_DSTORE_PREAD_OFFSET, offset);
	perfc_trace_attr(PEA_DSTORE_PREAD_COUNT, count);
	perfc_trace_attr(PEA_DSTORE_PREAD_BS, bs);

	rc = __dstore_pread(obj, offset, count, bs, buf);

	perfc_trace_attr(PEA_DSTORE_PREAD_RES_RC, rc);
	perfc_trace_finii(PERFC_TLS_POP_DONT_VERIFY);

	return rc;
}
