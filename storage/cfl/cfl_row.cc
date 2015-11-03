#include <my_byteorder.h>
#include "cfl.h"
#include "cfl_row.h"

inline cfl_dti_t
cfl_mytime2dti(const MYSQL_TIME &mtime)
{
  cfl_dt_t cdt;
  cdt.year = mtime.year;
  cdt.month = mtime.month;
  cdt.day = mtime.day;
  cdt.hour = mtime.hour;
  cdt.minute = mtime.minute;
  cdt.second = mtime.second;
  cdt.microsecond = mtime.second_part;
  return cfl_t2i(&cdt);
}

/*
  return:
  小于0，表示失败
  大于0，返回填入缓冲区的字节数。
*/
inline int
cfl_field_from_mysql(Field *field, void *buf, uint32_t buf_size)
{
  enum_field_types type = field->type();
  switch (type)
  {
  case MYSQL_TYPE_TINY:
  {
    int64_t v = field->val_int();
    int8_t v2 = (int8_t)v;
    endian_write_uint8(buf, (uint8_t)v2);
    return sizeof(uint8_t);
  }
  case MYSQL_TYPE_SHORT:
  {
    int64_t v = field->val_int();
    int16_t v2 = (int16_t)v;
    endian_write_uint16(buf, (uint16_t)v2);
    return sizeof(uint16_t);
  }
  case MYSQL_TYPE_LONG:
  {
    int64_t v = field->val_int();
    int32_t v2 = (int32_t)v;
    endian_write_uint32(buf, (uint32_t)v2);
    return sizeof(uint32_t);
  }
  case MYSQL_TYPE_LONGLONG:
  {
    int64_t v = field->val_int();
    endian_write_uint16(buf, (uint64_t)v);
    return sizeof(uint64_t);
  }
  case MYSQL_TYPE_TIMESTAMP:
  {
    //将mysql的时间戳转换为cfl的时间戳
    //参考field.cc:5043。Field_temporal_with_date::val_str
    MYSQL_TIME mtime;
    bool r;
    r = field->get_date(&mtime, 0);
    if (r)
    {
      //to do : 错误处理
    }
    cfl_dti_t cdti = cfl_mytime2dti(mtime);
    //cfl_dt_t cdt;
    //cfl_i2t(cdti, &cdt);
    if (buf_size < CFL_DTI_STORAGE_SIZE)
      return -1;
    cfl_dti2s(cdti, buf);
    return CFL_DTI_STORAGE_SIZE;
  }
  default:
  {
    //统一转换为字符串类型
    char attribute_buffer[1024];
    String attribute(attribute_buffer, sizeof(attribute_buffer),
                     &my_charset_bin);

    field->val_str(&attribute,&attribute);
    break;
  }
  }
  return 0;
}

uint32_t
cfl_row_from_mysql(Field ** fields, uchar *row
                   , void *row_buf, uint32_t buf_size, cfl_dti_t *key)
{
  uint32_t cfl_row_size = 0;
  uint8_t *offset = (uint8_t*)row_buf;
  uint32_t buf_size_inner = buf_size;
  int filled_length;

  *key = 1; //用于测试，需要删除

  for (Field **field = fields ; *field ; field++)
  {
    const bool is_null= (*field)->is_null();
    if (is_null)
    {
      //记录为null的情况
    }
    else
    {
      //读取数据，并转换为cfl的row格式
      filled_length = cfl_field_from_mysql(*field, offset, buf_size_inner);
      if (filled_length < 0)
      {
        //to do : 处理错误
      }
      offset += filled_length;
      buf_size_inner -= filled_length;
      cfl_row_size += filled_length;
    }
  }
  return cfl_row_size;
}

int
cfl_row_to_mysql(Field ** fields, uchar *buf, uchar *row)
{
  for (Field **field = fields ; *field ; field++)
  {
  }

  return 0;
}
