#ifndef _CFL_ROW_H_
#define _CFL_ROW_H_

#include "cfl_dt.h"
#include "cfl_endian.h"

/*
  将mysql的数据转换为cfl进行存储的行数据
  返回:转换后的行大小
*/
uint32_t
cfl_row_from_mysql(Field ** fields, uchar *row
                   , void *row_buf, uint32_t buf_size, cfl_dti_t *key);

/*
  将cfl的数据转换为mysql的数据
  返回:失败-1，成功转换后的行长度
*/
int
cfl_row_to_mysql(Field ** fields, uchar *buf, uchar *row, 
                 uint8_t * cfl_row, uint32_t row_length);

#endif //_CFL_ROW_H_
