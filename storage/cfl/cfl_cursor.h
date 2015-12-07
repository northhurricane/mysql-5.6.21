#ifndef _CFL_CURSOR_H_
#define _CFL_CURSOR_H_

#include <stdint.h>

#define CFL_CURSOR_BEFOR_START (0xFFFFFFFFFFFFFFFF)
#define CFL_CURSOR_AFTER_END   (0xFFFFFFFFFFFFFFFE)

class CflPage;

struct cfl_cursor_struct
{
  uint64_t position;  //1-based.行位置
  uint32_t page_no;   //0-based.当前页的编号
  CflPage  *page;     //当前页的内存对象
  uint32_t row_no; //0-based.当前页的当前record
  uint8_t  *row;   //fetch的record在内存页中的位置
  uint16_t row_length; //行长度
};
typedef cfl_cursor_struct cfl_cursor_t;

void cfl_cursor_init(cfl_cursor_t &cursor)
{
  cursor.position = CFL_CURSOR_BEFOR_START;

  cursor.page_no = 0;
  cursor.page = NULL;
  cursor.row_no = 0;
  cursor.row = NULL;
}

inline uint64_t cfl_cursor_position_get(cfl_cursor_t &cursor)
{
  return cursor.position;
}

inline void cfl_cursor_position_set(cfl_cursor_t &cursor, uint64_t pos)
{
  cursor.position = pos;
}

inline uint32_t cfl_cursor_page_no_get(cfl_cursor_t &cursor)
{
  return cursor.page_no;
}

inline void cfl_cursor_page_no_set(cfl_cursor_t &cursor, uint32_t page_no)
{
  cursor.page_no = page_no;
}

inline uint32_t cfl_cursor_row_no_get(cfl_cursor_t &cursor)
{
  return cursor.row_no;
}

inline void cfl_cursor_row_no_set(cfl_cursor_t &cursor, uint32_t row_no)
{
  cursor.row_no = row_no;
}

inline CflPage* cfl_cursor_page_get(cfl_cursor_t &cursor)
{
  return cursor.page;
}

inline void cfl_cursor_page_set(cfl_cursor_t &cursor, CflPage *page)
{
  cursor.page = page;
}

inline uint8_t*
cfl_cursor_row_get(cfl_cursor_t &cursor)
{
  return cursor.row;
}

inline void
cfl_cursor_row_set(cfl_cursor_t &cursor, uint8_t *row)
{
  cursor.row = row;
}

inline uint16_t
cfl_cursor_row_length_get(cfl_cursor_t &cursor)
{
  return cursor.row_length;
}

inline void
cfl_cursor_row_length_set(cfl_cursor_t &cursor, uint16_t row_length)
{
  cursor.row_length = row_length;
}

#endif //_CFL_CURSOR_H_
