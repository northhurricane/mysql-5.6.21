/*
存储单元，该单元将顺序存储
每个页面固定大小为1m，也就是1024*1024
*/

#include <string.h>
#include "cfl.h"
#include "cfl_page.h"
#include "cfl_table.h"


void
CflPageMaker::AddRow(void *row, uint16_t row_size)
{
  //将row写入缓冲区
  memcpy(row_offset_, row, row_size);

  rows_counter_++;
  row_offset_ += row_size;
  pos_offset_ -= CFL_PAGE_ROW_POS_SIZE;
  DBUG_ASSERT(row_offset_ <= pos_offset_);
}

void
CflPageMaker::Flush(CflStorage *storage)
{
  //写入存储对象
  storage->WritePage(buffer_, rows_counter_);

  Reset();
}




