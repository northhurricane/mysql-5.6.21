#ifndef _CFL_ROW_H_
#define _CFL_ROW_H_

#include "cfl_dt.h"
#include "cfl_endian.h"

uint32_t
cfl_row_from_mysql(Field ** fields, uchar *row
                   , void *row_buf, uint32_t buf_size, cfl_dti_t *key);

#endif //_CFL_ROW_H_
