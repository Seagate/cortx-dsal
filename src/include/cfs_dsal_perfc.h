/*
 * Filename:	cfs_perfc.h
 * Description:	This module defines performance counters and helpers.
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

#ifndef CFS_PERF_COUNTERS_H_
#define CFS_PERF_COUNTERS_H_
/******************************************************************************/
#include "perf/tsdb.h" /* ACTION_ID_BASE */
#include "operation.h"
#include <pthread.h>
#include <string.h>
#include "debug.h"
#include "perf/perf-counters.h"

enum perfc_function_tags {
	PFT_DS_START = PFTR_RANGE_3_START,

	PFT_DSTORE_GET,
	PFT_DSTORE_PREAD,
	PFT_DSTORE_PWRITE,

	PFT_DS_INIT,
	PFT_DS_FINISH,

	PFT_DS_OBJ_GET_ID,
	PFT_DS_OBJ_CREATE,
	PFT_DS_OBJ_DELETE,
	PFT_DS_OBJ_ALLOC,
	PFT_DS_OBJ_FREE,
	PFT_DS_OBJ_OPEN,
	PFT_DS_OBJ_CLOSE,

	PFT_DS_IO_INIT,
	PFT_DS_IO_SUBMIT,
	PFT_DS_IO_WAIT,
	PFT_DS_IO_FINISH,

	PFT_DSTORE_INIT,
	PFT_DSTORE_FINI,
	PFT_DSTORE_OBJ_CREATE,
	PFT_DSTORE_OBJ_DELETE,
	PFT_DSTORE_OBJ_SHRINK,
	PFT_DSTORE_OBJ_RESIZE,
	PFT_DSTORE_GET_NEW_OBJID,
	PFT_DSTORE_OBJ_OPEN,
	PFT_DSTORE_OBJ_CLOSE,
	PFT_DSTORE_IO_OP_INIT_AND_SUBMIT,
	PFT_DSTORE_IO_OP_WRITE,
	PFT_DSTORE_IO_OP_READ,
	PFT_DSTORE_IO_OP_WAIT,
	PFT_DSTORE_IO_OP_FINI,

	PFT_DS_END = PFTR_RANGE_3_END
}

enum perfc_entity_attrs {
	PEA_START = PEAR_RANGE_3_START,

	PEA_DSTORE_OLD_SIZE,
	PEA_DSTORE_NEW_SIZE,
	PEA_DSTORE_GET_RES_RC,
	PEA_DSTORE_PREAD_OFFSET,
	PEA_DSTORE_PREAD_COUNT,
	PEA_DSTORE_PREAD_RES_RC,
	PEA_DSTORE_PWRITE_OFFSET,
	PEA_DSTORE_PWRITE_COUNT,
	PEA_DSTORE_PWRITE_RES_RC,
	PEA_DSTORE_BS,

	PEA_TIME_ATTR_START_M0_OBJ_OP,
	PEA_TIME_ATTR_END_M0_OBJ_OP,
	PEA_TIME_ATTR_START_M0_OP_FINISH,
	PEA_TIME_ATTR_END_M0_OP_FINISH,
	PEA_TIME_ATTR_START_M0_OP_FREE,
	PEA_TIME_ATTR_END_M0_OP_FREE,
	PEA_M0_OP_SM_ID,
	PEA_M0_OP_SM_STATE,
	PEA_DSTORE_RES_RC,

	PEA_TIME_ATTR_START_M0_OBJ_INIT,
	PEA_TIME_ATTR_END_M0_OBJ_INIT,
	PEA_TIME_ATTR_START_M0_OP_LAUNCH,
	PEA_TIME_ATTR_END_M0_OP_LAUNCH,
	PEA_TIME_ATTR_START_M0_OP_WAIT,
	PEA_TIME_ATTR_END_M0_OP_WAIT,
	PEA_TIME_ATTR_START_M0_RC,
	PEA_TIME_ATTR_END_M0_RC,
	PEA_TIME_ATTR_START_M0_ENTITY_CREATE,
	PEA_TIME_ATTR_END_M0_ENTITY_CREATE,
	PEA_TIME_ATTR_START_M0_ENTITY_DELETE,
	PEA_TIME_ATTR_END_M0_ENTITY_DELETE,
	PEA_TIME_ATTR_START_M0_ENTITY_OPEN,
	PEA_TIME_ATTR_END_M0_ENTITY_OPEN,
	PEA_TIME_ATTR_START_M0_FREE,
	PEA_TIME_ATTR_END_M0_FREE,
	PEA_TIME_ATTR_START_M0_ENTITY_FINISH,
	PEA_TIME_ATTR_END_M0_ENTITY_FINISH,
	PEA_TIME_ATTR_START_M0_UFID_NEXT,
	PEA_TIME_ATTR_END_M0_UFID_NEXT,
	PEA_TIME_ATTR_START_M0_ALLOC_PTR,
	PEA_TIME_ATTR_END_M0_ALLOC_PTR,
	PEA_M0STORE_RES_RC,

	PEA_END = PEAR_RANGE_3_END
};

enum perfc_entity_maps {
	PEM_START = PEMR_RANGE_3_START,

	PEM_DSAL_TO_MOTR,
	PEM_DSTORE_TO_NFS,

	PEM_END = PEMR_RANGE_3_END
};
