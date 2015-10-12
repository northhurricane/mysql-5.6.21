#include "cfl_table.h"
#include "cfl_insert_buffer.h"
#include "cfl_page.h"

CflTable*
CflTable::Create()
{
  return 0;
}

int
CflTable::Destroy()
{
  return 0;
}

CflTable*
CflTable::Open()
{
  return 0;
}

int
CflTable::Close()
{
  return 0;
}

void
CflTable::Insert(cfl_dti_t key, void *row, uint16_t row_size)
{
  mysql_mutex_lock(&insert_buffer_mutex_);

  if (PageOverflow(row_size))
  {
    //将数据刷入磁盘
  }

  //插入行进入insert buffer
  insert_buffer_->Insert(key, row, row_size);

  //判断insert buffer的容量，是否需要写入磁盘
  mysql_mutex_unlock(&insert_buffer_mutex_);
}

bool
CflTable::PageOverflow(uint16_t row_size)
{
  //计算当前行是否导致存储内容超过cfl的page容量
  //计算行数所占中间
  uint32_t total_rows_size = row_size + insert_buffer_->GetRowsSize();
  //页面内索引所占空间
  uint32_t index_size = 
  (insert_buffer_->GetRowsCount() + 1) * CFL_PAGE_INDEX_UNIT_SIZE;
  //计算占用的所有空间
  uint32_t total_size = CFL_PAGE_FIX_SPACE_SIZE + total_rows_size + index_size;

  if (total_size <= CFL_PAGE_SIZE)
    return true;
  return false;
}
