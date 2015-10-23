#include "cfl_table.h"
#include "cfl_insert_buffer.h"
#include "cfl_page.h"

void
CflStorage::WritePage(void *page, uint32_t rows_count)
{
}

CflTable*
CflTable::Create()
{
  CflTable *table = new CflTable();
  int r = table->Initialize();
  if (r < 0)
  {
    return NULL;
  }

  return table;
}

int
CflTable::Destroy(CflTable *table)
{
  DBUG_ASSERT(table != NULL);

  table->Deinitialize();
  delete table;

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
    insert_buffer_->Flush(maker_, storage_);
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
  (insert_buffer_->GetRowsCount() + 1) * CFL_PAGE_ROW_POS_SIZE;
  //计算占用的所有空间
  uint32_t total_size = CFL_PAGE_FIX_SPACE_SIZE + total_rows_size + index_size;

  if (total_size > CFL_PAGE_SIZE)
    return true;
  return false;
}

int
CflTable::Initialize()
{
  mysql_mutex_init(NULL, &insert_buffer_mutex_, MY_MUTEX_INIT_FAST);
  insert_buffer_ = CflInsertBuffer::Create();
  if (insert_buffer_ == NULL)
  {
  }

  return 0;
FAIL:
  return -1;
}

int
CflTable::Deinitialize()
{
  mysql_mutex_destroy(&insert_buffer_mutex_);
  return 0;
}

