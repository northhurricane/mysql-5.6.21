#ifndef _CFL_H_
#define _CFL_H_

#include <my_global.h>
#include <my_dbug.h>
#include <field.h>
#include <stdint.h>

uint32_t
cfl_row_from_mysql(Field ** fields, uchar *row
                   , void *row_buf, uint32_t buf_size);

#endif //_CFL_H_

