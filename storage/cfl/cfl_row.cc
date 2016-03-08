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

inline void
cfl_dti2mytime(cfl_dti_t dti, MYSQL_TIME &mtime)
{
  cfl_dt_t dt;

  cfl_i2t(dti, &dt);

  mtime.year = dt.year;
  mtime.month = dt.month;
  mtime.day = dt.day;
  mtime.hour = dt.hour;
  mtime.minute = dt.minute;
  mtime.second = dt.second;
  mtime.second_part = dt.microsecond;
  mtime.time_type = MYSQL_TIMESTAMP_DATETIME;
}

inline bool
cfl_field_is_key(Field *field)
{
  enum_field_types type = field->type();
  if (type == MYSQL_TYPE_TIMESTAMP)
  {
    return true;
  }
  return false;
}

inline cfl_dti_t
cfl_row_get_key(uint8_t *key)
{
  cfl_dti_t dti;
  dti = cfl_s2dti(key);
  return dti;
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
    endian_write_uint64(buf, (uint64_t)v);
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
    /*{
    cfl_dt_t cdt;
    cfl_i2t(cdti, &cdt);
    printf("stop for debug");
    }*/
    if (buf_size < CFL_DTI_STORAGE_SIZE)
      return -1;
    cfl_dti2s(cdti, buf);
    return CFL_DTI_STORAGE_SIZE;
  }
  default:
  {
    //统一转换为字符串类型
    char value_buffer[1024];
    String value(value_buffer, sizeof(value_buffer),
                     &my_charset_bin);

    field->val_str(&value,&value);

    uint32_t str_len = value.length();
    uint8_t *offset = (uint8_t*)buf;
    endian_write_uint16(offset, str_len);
    offset += sizeof(uint16_t);
    memcpy(offset, value.ptr(), str_len);
    return sizeof(uint16_t) + str_len;
  }
  }
  return 0;
}

inline int
cfl_field_length(Field *field, uint8_t *field_data)
{
  enum_field_types type = field->type();
  switch (type)
  {
  case MYSQL_TYPE_TINY:
  {
    return sizeof(uint8_t);
  }
  case MYSQL_TYPE_SHORT:
  {
    return sizeof(uint16_t);
  }
  case MYSQL_TYPE_LONG:
  {
    return sizeof(uint32_t);
  }
  case MYSQL_TYPE_LONGLONG:
  {
    return sizeof(uint64_t);
  }
  case MYSQL_TYPE_TIMESTAMP:
  {
    return CFL_DTI_STORAGE_SIZE;
  }
  default:
  {
    //通过字符方式存储
    uint16_t str_len = endian_read_uint16(field_data);
    return str_len + sizeof(uint16_t);
  }
  }
  
  return 0;
}

/*
返回值:
  小于0，出现错误
  cfl字段（field）的长度
*/
inline int
cfl_field_2_mysql(Field *field, uint8_t *field_data)
{
  enum_field_types type = field->type();
  switch (type)
  {
  case MYSQL_TYPE_TINY:
  {
    int8_t v2 = (int8_t)123;
    //endian_write_uint8(buf, (uint8_t)v2);
    field->store(v2);
    return sizeof(uint8_t);
  }
  case MYSQL_TYPE_SHORT:
  {
    int16_t v2 = (int16_t)123;
    //endian_write_uint16(buf, (uint16_t)v2);
    field->store(v2);
    return sizeof(uint16_t);
  }
  case MYSQL_TYPE_LONG:
  {
    int32_t v2 = (int32_t)123;
    v2 = endian_read_uint32(field_data);
    field->store(v2);
    return sizeof(uint32_t);
  }
  case MYSQL_TYPE_LONGLONG:
  {
    int64_t v2 = (int64_t)123;
    //endian_write_uint16(buf, (uint64_t)v);
    field->store(v2);
    return sizeof(uint64_t);
  }
  case MYSQL_TYPE_TIMESTAMP:
  {
    MYSQL_TIME mtime;
    cfl_dti_t dti;

    dti = cfl_s2dti(field_data);
    cfl_dti2mytime(dti, mtime);
    field->store_time(&mtime);
    return CFL_DTI_STORAGE_SIZE;
    
    /*    mtime.time_type = MYSQL_TIMESTAMP_DATETIME;
    mtime.year = 2015; mtime.month = 11; mtime.day = 6;
    mtime.hour = 14; mtime.minute = 20; mtime.second = 20;
    mtime.second_part = 999;
    field->store_time(&mtime);
    return CFL_DTI_STORAGE_SIZE;*/
  }
  default:
  {
    //通过字符方式存储
    uint16_t str_len = endian_read_uint16(field_data);
    uint8_t *str = field_data + sizeof(uint16_t);

    field->store((const char*)str, str_len
                 , &my_charset_bin, CHECK_FIELD_IGNORE);

    return str_len + sizeof(uint16_t);
    /*
    char test[] = "abc";
    field->store(test, 3, &my_charset_bin, CHECK_FIELD_IGNORE);
    return 1;
    */
  }
  }
  
  return 0;
}

uint32_t
cfl_row_from_mysql(Field ** fields, uchar *row
                   , void *row_buf, uint32_t buf_size, cfl_dti_t *key)
{
  uint32_t cfl_row_size = 0;

  DBUG_ASSERT(key != NULL);
  *key = 1; //用于测试，可以删除

  //初始化row flag部分
  cfl_row_flag_t flag = 0;
  uint8_t *cfl_flag = (uint8_t*)row_buf;
  cfl_row_flag_write(cfl_flag, flag);
  cfl_row_size += CFL_ROW_STORAGE_SIZE;

  //进行数据填写
  uint8_t *offset = (uint8_t*)row_buf + CFL_ROW_STORAGE_SIZE;
  uint32_t buf_size_inner = buf_size - CFL_ROW_STORAGE_SIZE;
  int filled_length;
  bool key_field = false;
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
      key_field = cfl_field_is_key(*field);
      if (key_field)
      {
        *key = cfl_row_get_key(offset);
      }
      offset += filled_length;
      buf_size_inner -= filled_length;
      cfl_row_size += filled_length;
    }

  }
  return cfl_row_size;
}

int
cfl_row_to_mysql(Field ** fields, uchar *buf, uchar *row, 
                 uint8_t *cfl_row, uint32_t row_length)
{
  //读取flag信息
  uint8_t *cfl_flag = cfl_row;
  cfl_row_flag_t flag;
  flag = cfl_row_flag_read(cfl_flag);

  //读取数据内容
  uint8_t *cfl_field = cfl_row + CFL_ROW_STORAGE_SIZE;
  int field_length = 0;

  for (Field **field = fields ; *field ; field++)
  {
    field_length = cfl_field_2_mysql(*field, cfl_field);
    //下一个字段（field）
    cfl_field += field_length;
  }

  return 0;
}

void*
cfl_row_get_nth_field(Field ** fields, uint8_t *cfl_row, uint32_t row_length
                      , uint32_t nth_field, uint32_t *field_len)
{
  uint8_t *cfl_field = cfl_row;
  int field_length = 0;
  uint32_t counter = 0;

  *field_len = 0;
  for (Field **field = fields ; *field ; field++)
  {
    field_length = cfl_field_length(*field, cfl_field);

    if (counter == nth_field)
      break;

    counter++;
    cfl_field += field_length;
  }
  DBUG_ASSERT(counter == nth_field);

  *field_len = field_length;
  return cfl_field;
}

cfl_dti_t
cfl_row_get_key_data(Field **fields, uint8_t *cfl_row)
{
  uint8_t *cfl_field = cfl_row;
  int field_length = 0;
  Field *rfield;
  cfl_dti_t rowkey = 0;

  for (Field **field = fields ; *field ; field++)
  {
    rfield = *field;
    if (strcmp(rfield->field_name, CFL_INDEX_TIMESTAMP_NAME) == 0)
    {
      DBUG_ASSERT(rfield->decimals() == CFL_INDEX_TIMESTAMP_SCALE);
      rowkey = cfl_row_get_key(cfl_field);
      break;
    }
    field_length = cfl_field_length(*field, cfl_field);
    //下一个字段（field）
    cfl_field += field_length;
  }

  DBUG_ASSERT(rowkey != 0);
  return rowkey;
}
