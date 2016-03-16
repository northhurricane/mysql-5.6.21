#ifndef _CFL_CURSOR_H_
#define _CFL_CURSOR_H_

#include <stdint.h>

#define CFL_CURSOR_BEFOR_START (0xFFFFFFFFFFFFFFFF)
#define CFL_CURSOR_AFTER_END   (0xFFFFFFFFFFFFFFFE)

#include "cfl.h"
#include "cfl_page.h"

class CflStorage;

/*
  index search struct
*/
struct cfl_isearch_struct
{
  cfl_dti_t key;
  enum cfl_key_cmp key_cmp;  
};
typedef struct cfl_isearch_struct cfl_isearch_t;

/*
  用于进行记录定位
*/
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

inline void cfl_cursor_init(cfl_cursor_t &cursor)
{
  cursor.position = CFL_CURSOR_BEFOR_START;

  cursor.page_no = 0;
  cursor.page = NULL;
  cursor.row_no = 0;
  cursor.row = NULL;
}

inline uint64_t cfl_cursor_position_get(const cfl_cursor_t &cursor)
{
  return cursor.position;
}

inline void cfl_cursor_position_set(cfl_cursor_t &cursor, uint64_t pos)
{
  cursor.position = pos;
}

inline uint32_t cfl_cursor_page_no_get(const cfl_cursor_t &cursor)
{
  return cursor.page_no;
}

inline void cfl_cursor_page_no_set(cfl_cursor_t &cursor, uint32_t page_no)
{
  cursor.page_no = page_no;
}

inline uint32_t cfl_cursor_row_no_get(const cfl_cursor_t &cursor)
{
  return cursor.row_no;
}

inline void cfl_cursor_row_no_set(cfl_cursor_t &cursor, uint32_t row_no)
{
  cursor.row_no = row_no;
}

inline CflPage* cfl_cursor_page_get(const cfl_cursor_t &cursor)
{
  return cursor.page;
}

inline void cfl_cursor_page_set(cfl_cursor_t &cursor, CflPage *page)
{
  cursor.page = page;
}

inline uint8_t*
cfl_cursor_row_get(const cfl_cursor_t &cursor)
{
  return cursor.row;
}

inline void
cfl_cursor_row_set(cfl_cursor_t &cursor, uint8_t *row)
{
  cursor.row = row;
}

inline uint16_t
cfl_cursor_row_length_get(const cfl_cursor_t &cursor)
{
  return cursor.row_length;
}

inline void
cfl_cursor_row_length_set(cfl_cursor_t &cursor, uint16_t row_length)
{
  cursor.row_length = row_length;
}

inline void
cfl_cursor_fill_row(cfl_cursor_t &cursor)
{
  uint8_t *row;
  void *page_data;
  uint32_t row_length;

  DBUG_ASSERT(cursor.page != NULL);
  page_data = cursor.page->page();
  row = cfl_page_nth_row((uint8_t*)page_data, cursor.row_no);
  row_length = cfl_page_nth_row_length((uint8_t*)page_data, cursor.row_no);
  cfl_cursor_row_set(cursor, row);
  cfl_cursor_row_length_set(cursor, row_length);
}


inline bool isearch_key_match(const cfl_isearch_t &isearch, cfl_dti_t rowkey)
{
  //比较该记录的key，是否符合isearch中比较的。如果不符合，则说明所有记录已经
  //完成获取。将cursor的position设置为CFL_CURSOR_AFTER_END
  bool matched;
  switch (isearch.key_cmp)
  {
  case KEY_EQUAL:
    if (rowkey != isearch.key)
      matched = false;
    else
      matched = true;
    break;
  case KEY_GE:
  case KEY_G:
    matched = true;
    break;
  case KEY_LE:
    if (rowkey <= isearch.key)
      matched = true;
    else
      matched = false;
    break;
  case KEY_L:
    if (rowkey < isearch.key)
      matched = true;
    else
      matched = false;
    break;
  default:
    DBUG_ASSERT(false);
    matched = false;
  }
  return matched;
}

/*
  清除cursor上的资源
*/
inline void cfl_cursor_clear(cfl_cursor_t &cursor)
{
  CflPage *page = cfl_cursor_page_get(cursor);
  if (page != NULL)
    CflPageManager::PutPage(page);
  cfl_cursor_page_set(cursor, NULL);
}

/*
  在指定的存储对象上进行搜索定位
  参数:
    storage,进行定位的存储对象
    cursor ,定位信息保存的位置
    isearch,进行定位比较的信息
*/
int
cfl_cursor_locate(CflStorage *storage, Field **field
                  , cfl_cursor_t &cursor, cfl_isearch_t isearch);

int
cfl_cursor_locate_next(CflStorage *storage, Field **field
                       , cfl_cursor_t &cursor , cfl_isearch_t isearch
                       , bool &over);
/////////////////////////////////
inline void
cfl_isearch_init(cfl_isearch_t &isearch)
{
  isearch.key = 0;
  isearch.key_cmp = KEY_EQUAL;
}

#endif //_CFL_CURSOR_H_
