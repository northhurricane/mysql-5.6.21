#include "cfl.h"

static void
cfl_row_from_mysql(Field *field)
{
  enum_field_types type = field->type();
  switch (type)
  {
    //转换为cfl的行数据
  case MYSQL_TYPE_TINY:
    break;
  case MYSQL_TYPE_SHORT:
    break;
  case MYSQL_TYPE_LONG:
    break;
  case MYSQL_TYPE_TIMESTAMP:
    //将mysql的时间戳转换为cfl的时间戳
    //参考field.cc:5043。Field_temporal_with_date::val_str
    break;
  default:
    //统一转换为字符串类型
    break;
  }
}

uint32_t
cfl_row_from_mysql(Field ** fields, uchar *row
                   , void *row_buf, uint32_t buf_size)
{
  uint32_t cfl_row_size = 0;
  uint32_t offset = 0;
  char attribute_buffer[1024];
  String attribute(attribute_buffer, sizeof(attribute_buffer),
                   &my_charset_bin);
  for (Field **field = fields ; *field ; field++)
  {
    const bool is_null= (*field)->is_null();
    if (is_null)
    {
      //记录为null的情况
    }
    else
    {
      //获取字符串内容
      (*field)->val_str(&attribute, &attribute);
    }
  }
  return cfl_row_size;
}
