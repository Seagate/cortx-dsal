/*
 * Filename:         dstore.h
 * Description:      data store module of DSAL
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

/* This file describes the public API of DSTORE module -- DSAL
 * (data store abstraction layer).
 *
 * Overview
 * --------
 *
 * DSAL exposes the following mechanisms:
 *	- initialization/finalization of the DSTORE;
 *	- creation and removal of DSTORE objects;
 *	- state management (open/close);
 *	- IO operations;
 *	- various utility functions exposed by DSTORE backends;
 *
 * State management
 * ----------------
 *
 * DSAL allows its users to create an in-memory representation of an object
 * and destoroy it using the corresponding open/close calls.
 *
 * Right now, DSTORE does not provide any guarantees regarding synchronization
 * of the objects stored in the stable storage and their in-memory
 * representations except very simple cases. For example, open() call will
 * return an error if DSTORE was not properly initialized (errno depends on
 * the backend) or if the requested object does not exist (ENOENT). However,
 * an attempt to concurrently delete and open an object is considered to be
 * undefined behavior. It is done deliberately because some of the users
 * of DSAL (for example, S3 server) does not require strong consistency.
 * In other words, DSAL provides a mechanism for object storage management
 * while the policies (consistency, concurrency) should be implemented
 * in the upper layers.
 */
#ifndef _DSTORE_H
#define _DSTORE_H

#include <stddef.h> /* size_t */
#include <stdint.h> /* uint*_t */
#include <stdbool.h> /* bool */
#include <sys/types.h> /* off_t */
#include <sys/stat.h> /* stat */
#include <object.h> /* obj_id_t */

struct collection_item;

typedef obj_id_t dstore_oid_t;

struct dstore;

/** Base data type that represents a data-store object in open state. */
struct dstore_obj;

/** Base data type that represents an IO operation. */
struct dstore_io_op;

/* see the description in dstore_bufvec.h */
struct dstore_io_vec;

struct dstore_extent_vec;

int dstore_init(struct collection_item *cfg, int flags);

int dstore_fini(struct dstore *dstore);

/*
 * @todo: https://github.com/Seagate/cortx-dsal/issues/4
 */
int dstore_obj_create(struct dstore *dstore, void *ctx,
		      dstore_oid_t *oid);

int dstore_obj_delete(struct dstore *dstore, void *ctx,
		      dstore_oid_t *oid);

/* Deprecated: should be removed when dstore_io_op_read is fully implemented. */
int dstore_obj_read(struct dstore *dstore, void *ctx,
		    dstore_oid_t *oid, off_t offset,
		    size_t buffer_size, void *buffer, bool *end_of_file,
		    struct stat *stat);

/* Deprecated: should be removed when dstore_io_op_write is fully implemented. */
int dstore_obj_write(struct dstore *dstore, void *ctx,
		     dstore_oid_t *oid, off_t offset,
		     size_t buffer_size, void *buffer, bool *fsal_stable,
		     struct stat *stat);

/* This API based on input size decides whether this is object shrink operation,
 * extend operation or noop in case of both the size are same
 * In case of noop/extend operation nothing needs to be done on particular
 * object, extend is considered as hole and while reading user should be
 * receiving all zeros for this range.
 * In case of shrink, all zeroes will be written to backend obj store for given
 * specified range
 * @paramp[in] obj - An open object.
 * @paramp[in] old_size - Current size of given object
 * @paramp[in] new_size - New size of a given object after performing this op
 * @return 0 on success -errno given by backend operation
 */
int dstore_obj_resize(struct dstore_obj *obj, size_t old_size, size_t new_size);

int dstore_get_new_objid(struct dstore *dstore, dstore_oid_t *oid);

/** Prepare object for IO operation.
 * This function prepares a data-store object for IO operations.
 * It may involve internal IO (for example to fetch the object
 * layout, to check object existence etc) and/or syscalls
 * (for example, POSIX open or POSIX read).
 * @param[in]  oid Object ID (u128) of object in "dstore".
 * @param[out] out In-memory representation of an open object.
 * @return 0 or -errno.
 */
int dstore_obj_open(struct dstore *dstore,
		    const dstore_oid_t *oid,
		    struct dstore_obj **out);

/** Release resources associated with an open object.
 * This function de-allocates memory and any other sources taken by
 * dstore_obj_open call.
 * The function blocks on a call to the underlying storage until
 * all the in-flight IO operations are stable. This behavior provides
 * open-to-close consistency.
 * An attempt to close already closed object causes undefined
 * behavior (double-free).
 * @param[in] obj Open object.
 * @return 0 or -errno.
 */
int dstore_obj_close(struct dstore_obj *obj);

/** A user-provided callback to receive notifications. */
typedef void (*dstore_io_op_cb_t)(void *cb_ctx,
				  struct dstore_io_op *op,
				  int op_rc);

struct dstore *dstore_get(void);

/* This api returns the block size from motr */
ssize_t dstore_get_bsize(struct dstore *dstore, dstore_oid_t *oid);

/** This API based on input decides whether the givevn request is aligned or not
 * If it is aligned request it will directly write the requested amount of data
 * to backend. If it is un-aligned then left and right aligned blocks might be
 * read-modify in to intermediate buffer location. Also rest of the aligned
 * blocks are being copied to intermediate buffer location to form uniform
 * aligned write request, and same is issued to the backend.
 * @paramp[in] obj - An open object.
 * @paramp[in] offset - An offset from where write needs to be done.
 * @paramp[in] count - Amount of data to be written
 * @paramp[in] bs - A minimum block size on which backend operates
 * @paramp[in] buf - Buffer from which data needs to be written to backend
 */
int dstore_pwrite(struct dstore_obj *obj, off_t offset, size_t count,
			size_t bs, char *buf);

/** This function based on input decides whether the given request is aligned or
 * unaligned I/O. In case of aligned I/O it will simply issue a read request as
 * it is to backend, data directly saved in to a requested buffer. In case of
 * unaligned I/O based on calculation an extra left or right aligned block might
 * be read in temporary allocated buffer. Required amount of data is extracted
 * from blocks being read and copied to reqested buffer. For rest of the aligned
 * blocks in between the left and right un-aligned block, aligned read will be
 * issued to backend and data is directly saved in to requested buffer.
 * @param[in] obj - An open object.
 * @param[in] offset - An offset from where read needs to be done.
 * @param[in] count  - Amount of data to be read.
 * @param[in] bs - A minimum block size on which backend store operates.
 * @param[in, out] buf - Buffer for storing the requested data.
 * @return 0 or -errno.
 */
int dstore_pread(struct dstore_obj *obj, off_t offset, size_t count,
		       size_t bs, char *buf);

/** This API does the handling for truncating a file
 *  @param[in] obj - An open object.
 *  @param[in] vec - vector filled with offset and count.
 *  @return 0 or -errno.
 */
int dstore_ptrunc(struct dstore_obj *obj, struct dstore_extent_vec *vec);
#endif
