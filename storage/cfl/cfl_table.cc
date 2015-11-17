#include "cfl_table.h"
#include "cfl_index.h"
#include "cfl_data.h"
#include "cfl_insert_buffer.h"
#include "cfl_page.h"

/*CflStorage*/
int
CflStorage::WritePage(void *page, uint32_t rows_count, cfl_dti_t dti)
{
  data_->WritePage(page, CFL_PAGE_SIZE);
  index_->AddPage(dti);

  return 0;
}

int
CflStorage::ReadPage(void *buffer, uint32_t buffer_size, uint32_t nth_page)
{
  return data_->ReadPage(buffer, buffer_size, nth_page);
}

int
CflStorage::CreateStorage(const char *name)
{
  CflIndex::CreateIndexStorage(name);
  CflData::CreateDataStorage(name);

  return 0;
}

int
CflStorage::DestroyStorage(const char *name)
{
  CflIndex::DestroyIndexStorage(name);
  CflData::DestroyDataStorage(name);

  return 0;
}

CflStorage*
CflStorage::Open(const char *name)
{
  CflStorage *storage;

  storage = new CflStorage();

  storage->Initialize(name);

  return storage;
}

int
CflStorage::Close(CflStorage *storage)
{
  storage->Deinitialize();

  delete storage;

  return 0;
}

int
CflStorage::Initialize(const char *name)
{
  index_ = CflIndex::Create(name);
  uint32_t index_node_number = index_->ReadIndexNodeNumber();
  data_ = CflData::Create(name, index_node_number);

  return 0;

  //to do : fail process
}

int
CflStorage::Deinitialize()
{
  return 0;
}

/*CflTable*/
int
CflTable::CreateStorage(const char *name)
{
  return CflStorage::CreateStorage(name);
}

int
CflTable::DestroyStorage(const char *name)
{
  return CflStorage::DestroyStorage(name);
}

CflTable*
CflTable::Create(const char *name)
{
  CflTable *table = new CflTable();
  int r = table->Initialize(name);
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
  //页面内索引所占空间。由于page的设计原因，包括当前行的index entry，还要增价一个index entry的空间。参考cfl_page.cc
  uint32_t index_size = 
  (insert_buffer_->GetRowsCount() + 2) * CFL_PAGE_ROW_POS_SIZE;
  //计算占用的所有空间
  uint32_t total_size = CFL_PAGE_FIX_SPACE_SIZE + total_rows_size + index_size;

  //测试用
  if (total_size > 80)
    return true;

  if (total_size > CFL_PAGE_SIZE)
    return true;
  return false;
}

int
CflTable::Initialize(const char *name)
{
  mysql_mutex_init(NULL, &insert_buffer_mutex_, MY_MUTEX_INIT_FAST);
  insert_buffer_ = CflInsertBuffer::Create();
  if (insert_buffer_ == NULL)
  {
    goto FAIL;
  }
  maker_ = CflPageMaker::Create();
  if (maker_ == NULL)
  {
    goto FAIL;
  }
  storage_ = CflStorage::Open(name);
  if (storage_ == NULL)
  {
    goto FAIL;
  }

  return 0;
FAIL:
  if (insert_buffer_ != NULL)
    CflInsertBuffer::Destroy(insert_buffer_);
  if (maker_ != NULL)
    CflPageMaker::Destroy(maker_);
  if (storage_ != NULL)
    CflStorage::Close(storage_);
  return -1;
}

int
CflTable::Deinitialize()
{
  mysql_mutex_destroy(&insert_buffer_mutex_);
  CflPageMaker::Destroy(maker_);
  CflInsertBuffer::Destroy(insert_buffer_);
  CflStorage::Close(storage_);

  return 0;
}

