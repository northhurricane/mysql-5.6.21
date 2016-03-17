#include "cfl_cursor.h"
#include "cfl_table.h"

inline bool
check_renext(const cfl_cursor_t &cursor)
{
  cfl_row_flag_t flag;
  uint8_t *row;

  row = cfl_cursor_row_get(cursor);
  flag = cfl_row_flag_read(row);
  if (cfl_row_flag_read_deleted(flag))
    return true;
  return false;
}


inline bool
locate_key_match(const cfl_cursor_t &cursor
                 , const cfl_isearch_t &isearch
                 , Field **field)
{
  cfl_dti_t rowkey;
  uint8_t *row = cfl_cursor_row_get(cursor);
  rowkey = cfl_row_get_key_data(field, row);
  return isearch_key_match(isearch, rowkey);
}


inline int
next_physical_row(CflStorage *storage, cfl_cursor_t &cursor, bool &over)
{
  uint32_t page_no;  //进行定位的页号
  uint32_t row_no;   //进行定位的行号
  CflPage *page = NULL;

  //获取下一条记录
  /*
    有如下情况
    1、下一条记录在当前页
    2、下一条记录在下一页。此时又存在两种情况
      2-1、下一页存在。如果记录页存在，其中必有记录，next一定会获得一条记录
      2-2、下一页不存在。说明已经完成所有记录的next
  */
  page_no = cfl_cursor_page_no_get(cursor);
  row_no = cfl_cursor_row_no_get(cursor);

  page = cfl_cursor_page_get(cursor);
  DBUG_ASSERT(page != NULL);
  void *page_data = page->page();
  uint32_t row_count = cfl_page_read_row_count(page_data);
  over = false;
  //假设当前页存在下一条记录。情况1
  row_no++;
  //判断情况1是否成立
  if (row_no >= row_count)
  {
    DBUG_ASSERT(row_no == row_count);//理论上不存在大于情况
    //当前页不存在下一条记录。情况2
    //行不在当前页，释放cursor当前的页，取得下一页
    CflPageManager::PutPage(page);
    page_no++;
    row_no = 0;
    page = CflPageManager::GetPage(storage, page_no);
    //后续页是否存在
    if (page == NULL)
    {
      //情况2-2
      over = true;
    }
  }

  if (!over)
  {
    //page_no, row_no, page这三个，确定行
    cfl_cursor_page_set(cursor, page);
    cfl_cursor_page_no_set(cursor, page_no);
    cfl_cursor_row_no_set(cursor, row_no);
    //获取其他相关信息
    uint8_t *row;
    void *page_data;
    uint32_t row_length;

    page_data = page->page();
    row = cfl_page_nth_row((uint8_t*)page_data, row_no);
    row_length = cfl_page_nth_row_length((uint8_t*)page_data, row_no);
    cfl_cursor_row_set(cursor, row);
    cfl_cursor_row_length_set(cursor, row_length);
  }

  return 0;
}

int
cfl_cursor_locate(CflStorage *storage, Field **field
                  , cfl_cursor_t &cursor, cfl_isearch_t isearch)
{
  int rc = 0;

  DBUG_ASSERT(cfl_cursor_position_get(cursor) == CFL_CURSOR_BEFOR_START);

  bool matched = false;
  matched = storage->LocateRow(isearch.key , isearch.key_cmp
                               , cursor, field);

  if (matched)
  {
    //由于存在定位到删除记录上，所以需要拨动定位信息，定位到一个恰当的记录
    bool renext;
    bool over = false;
    //定位第一个可供比较的记录
    do
    {
      renext = check_renext(cursor);
      if (renext)
        rc = next_physical_row(storage, cursor, over);
      else
        break;
    }
    while  (!over);

    //对定位到的记录，重新做匹配检查
    if (over)
      matched = false;
    else
      matched = locate_key_match(cursor, isearch, field);
  }

  //检查是否匹配到
  if (!matched)
  {
    cfl_cursor_clear(cursor);
    cfl_cursor_position_set(cursor, CFL_CURSOR_AFTER_END);
    int rc= HA_ERR_END_OF_FILE;
    return rc;
  }

  return rc;
}

int
cfl_cursor_locate_next(CflStorage *storage, Field **field
                       , cfl_cursor_t &cursor , cfl_isearch_t isearch
                       , bool &over)
{
  uint32_t page_no_orig;  //当前的页号
  uint32_t row_no_orig;   //当前的页号
  int rc = 0;

  page_no_orig = cfl_cursor_page_no_get(cursor);
  row_no_orig = cfl_cursor_row_no_get(cursor);

  /*
    locate next必须是locate_cursor函数定位后，才能被调用
    分为3种情况
    1、还未进行next操作，此时cursor的postion为CFL_CURSOR_BEFOR_START。
       由于在locate_cursor中定位到第一条数据，此时只需要根据cursor中的信息，
       进行数据获取即可。
    2、已经获取完最后一条数据，此时postion为CFL_CURSOR_AFTER_END。
       直接返回表明结果获取结束。
    3、获取下一条记录。此时cursor的postion记录的是上一条被获取的记录
  */
  //根据postion进行next的初始化
  over = false;
  if (cfl_cursor_position_get(cursor) == CFL_CURSOR_BEFOR_START)
  {
    /*情况1处理*/
    DBUG_ASSERT(cfl_cursor_page_get(cursor) != NULL);
    cfl_cursor_position_set(cursor, 1);
    cfl_cursor_fill_row(cursor);
    return 0;
  }
  else if (cfl_cursor_position_get(cursor) == CFL_CURSOR_AFTER_END)
  {
    /*情况2处理*/
    over = true;
    return 0;
  }
  else
  {
    /*情况3处理*/
    //获取下一条记录的位置信息
  }

  do
  {
    //获取下一条记录
    rc = next_physical_row(storage, cursor, over);
    if (!rc)
    {
      //错误处理
    }
    if (over)
    {
      //不存在locate的下一条记录
      break;
    }
    bool renext = false;
    renext = check_renext(cursor);
    if (renext)
      continue;
    bool matched = false;
    matched = locate_key_match(cursor, isearch, field);
    if (matched)
    {
      over = false;
      break;
    }
  }
  while (true);

  if (over)
  {
    //释放资源，设置获取前的页面和行信息
    CflPage *page;
    page = cfl_cursor_page_get(cursor);
    if (page != NULL)
      CflPageManager::PutPage(page);
    cfl_cursor_page_set(cursor, NULL);
    cfl_cursor_page_no_set(cursor, page_no_orig);
    cfl_cursor_row_no_set(cursor, row_no_orig);
    cfl_cursor_position_set(cursor, CFL_CURSOR_AFTER_END);
  }
  else
  {
    //更新position信息
    uint64_t position = 0;
    position = cfl_cursor_position_get(cursor);
    position++;
    cfl_cursor_position_set(cursor, position);
  }

  return 0;
}

int
cfl_cursor_next(CflStorage *storage, cfl_cursor_t &cursor, bool &over)
{
  /*page_no, row_no, page此定位最后的记录*/
  uint32_t page_no;
  uint32_t row_no;
  CflPage *page = NULL;
  /*
    算法说明
      page_no和row_no进行下一行的定位标识
      进入next的时候存在3种情况
        1、从未获取过记录，此时cursor的postion为CFL_CURSOR_BEFOR_START
        2、所有记录已经获取，此时cursor的postion为CFL_CURSOR_AFTER_END
        3、获取其中记录。此时cursor的postion为获取的记录数
    实现说明
      CflPage的申请和释放要注意，不要造成未释放的页面存在
  */
  over = false;
  if (cfl_cursor_position_get(cursor) == CFL_CURSOR_BEFOR_START)
  {
    /*情况1处理*/
    page_no = 0;
    row_no = 0;
    page = CflPageManager::GetPage(storage, page_no);
    //尝试获取第一页，如果不存在，则设为CFL_CURSOR_AFTER_END
    if (page == NULL)
    {
      over = true;
      cfl_cursor_position_set(cursor, CFL_CURSOR_AFTER_END);
      return 0;
    }
    cfl_cursor_page_set(cursor, page);
    cfl_cursor_page_no_set(cursor, page_no);
    cfl_cursor_row_no_set(cursor, row_no);
    cfl_cursor_fill_row(cursor);

    //判断当前cursor需要拨动指针到下一条
    bool renext = check_renext(cursor);
    if (!renext)
    {
      cfl_cursor_position_set(cursor, 1);
      return 0;
    }
  }
  else if (cfl_cursor_position_get(cursor) == CFL_CURSOR_AFTER_END)
  {
    /*情况2处理*/
    over = true;
    return 0;
  }

  int rc;
  do
  {
    rc = next_physical_row(storage, cursor, over);
    if (!rc)
    {
      //错误处理
    }
    if (over)
    {
      //不存在下一条记录
      break;
    }
    bool renext = false;
    renext = check_renext(cursor);
    if (!renext)
      break;
  } while (true);

  if (over)
  {
    cfl_cursor_clear(cursor);
    cfl_cursor_page_set(cursor, NULL);
    cfl_cursor_page_no_set(cursor, 0);
    cfl_cursor_row_no_set(cursor, 0);
    cfl_cursor_position_set(cursor, CFL_CURSOR_AFTER_END);
  }
  else
  {
    uint64_t position = 0;
    position = cfl_cursor_position_get(cursor);
    position++;
    cfl_cursor_position_set(cursor, position);
  }

  return rc;
}
