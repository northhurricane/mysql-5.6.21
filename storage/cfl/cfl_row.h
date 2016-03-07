#ifndef _CFL_ROW_H_
#define _CFL_ROW_H_

#include <stdint.h>
#include "cfl_dt.h"
#include "cfl_endian.h"

/*
cfl的行设计
version1第一版
只存储数据，没有row的控制信息

version2第二版
在头部增加1字节，用于存储控制信息，
*/

/*
行格式
row format:
|flag info|data           |
|8bits    |variable length|
flag info
|deleted  |reserve |
|1bit     |7bits   |
*/

typedef uint8_t cfl_row_flag_t;

#define CFL_ROW_STORAGE_SIZE (sizeof(cfl_row_flag_t))

#define CFL_ROW_FLAG_MASK_DELETED (0x8)

inline bool
cfl_row_flag_read_deleted(cfl_row_flag_t flag)
{
  if (CFL_ROW_FLAG_MASK_DELETED & flag)
    return true;
  return false;
}

inline cfl_row_flag_t
cfl_row_flag_write_deleted(cfl_row_flag_t flag, bool deleted)
{
  if (deleted)
    return CFL_ROW_FLAG_MASK_DELETED | flag;
  return flag;
}

inline void
cfl_row_flag_write(uint8_t * target, cfl_row_flag_t flag)
{
  *target = flag;
}

inline cfl_row_flag_t cfl_row_flag_read(uint8_t *source)
{
  return *source;
}

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


/*
  获取cfl格式的row中的数据
  参数
    nth:0-based
*/
void*
cfl_row_get_nth_field(Field ** fields, uint8_t *cfl_row, uint32_t row_length
                      , uint32_t nth_field, uint32_t *field_len);

cfl_dti_t
cfl_row_get_key_data(Field ** fields, uint8_t *cfl_row);

#endif //_CFL_ROW_H_
