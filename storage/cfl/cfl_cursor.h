#ifndef _CFL_CURSOR_H_
#define _CFL_CURSOR_H_

#include <stdint.h>

#define CFL_CURSOR_BEFOR_START (0xFFFFFFFF)
#define CFL_CURSOR_AFTER_END   (0xFFFFFFFE)

class CflPage;

struct cfl_cursor_struct
{
  uint64_t position;  //行位置
  uint32_t page_no;   //0-based.当前页的编号
  CflPage  *page;     //当前页的内存对象
  uint32_t rows_count;//当前页的记录数
  uint32_t row_no; //0-based.当前页的当前record
  uint8_t  *row;   //fetch的record在内存页中的位置

  uint64_t counter;   //记录操作次数
};
typedef cfl_cursor_struct cfl_cursor_t;

void cfl_cursor_init(cfl_cursor_t &cursor)
{
  cursor.position = CFL_CURSOR_BEFOR_START;

  cursor.page_no = 0;
  cursor.page = NULL;
  cursor.rows_count = 0;
  cursor.row_no = 0;
  cursor.row = NULL;

  cursor.counter = 0;
}

uint64_t cfl_cursor_counter_get(cfl_cursor_t &cursor)
{
  return cursor.counter;
}

uint64_t cfl_cursor_counter_inc(cfl_cursor_t &cursor)
{
  return cursor.counter++;
}

uint64_t cfl_cursor_position_get(cfl_cursor_t &cursor)
{
  return cursor.position;
}

void cfl_cursor_position_set(cfl_cursor_t &cursor, uint64_t pos)
{
  cursor.position = pos;
}

uint32_t cfl_cursor_page_no_get(cfl_cursor_t &cursor)
{
  return cursor.page_no;
}

void cfl_cursor_page_no_set(cfl_cursor_t &cursor, uint32_t page_no)
{
  cursor.page_no = page_no;
}

uint32_t cfl_cursor_row_no_get(cfl_cursor_t &cursor)
{
  return cursor.row_no;
}

void cfl_cursor_row_no_set(cfl_cursor_t &cursor, uint32_t row_no)
{
  cursor.row_no = row_no;
}

CflPage* cfl_cursor_page_get(cfl_cursor_t &cursor)
{
  return cursor.page;
}

void cfl_cursor_page_set(cfl_cursor_t &cursor, CflPage *page)
{
  cursor.page = page;
}

#endif //_CFL_CURSOR_H_
