#ifndef _CFL_H_
#define _CFL_H_

#include <my_global.h>
#include <my_dbug.h>
#include <field.h>
#include <stdint.h>

enum cfl_key_cmp {
  KEY_INVALID,       /* invalid status */
  KEY_EQUAL,         /* Find record equal to the key, else error */
  KEY_GE,            /* Record greater or equal  */
  KEY_LE,            /* Record less or equal */
  KEY_G,             /* Record greater */
  KEY_L,             /* Record less */
};

inline enum cfl_key_cmp
my_key_func2cfl_key_cmp(enum ha_rkey_function key_func)
{
  switch (key_func)
  {
  case HA_READ_KEY_EXACT:
    return KEY_EQUAL;
  case HA_READ_KEY_OR_NEXT:
    return KEY_GE;
  case HA_READ_KEY_OR_PREV:
    return KEY_LE;
  case HA_READ_AFTER_KEY:
    return KEY_G;
  case HA_READ_BEFORE_KEY:
    return KEY_L;
  default :
    return KEY_INVALID;
  }
  return KEY_INVALID;
}

#define CFL_LOCATE_PAGE_NULL (0xFFFFFFFF)

#define CFL_LOCATE_ROW_NULL (0xFFFFFFFF)

#define CFL_INDEX_TIMESTAMP_NAME "key_timestamp"
#define CFL_INDEX_TIMESTAMP_SCALE 6

#endif //_CFL_H_
