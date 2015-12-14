/*
存储单元，该单元将顺序存储
每个页面固定大小为1m，也就是1024*1024
*/

#include <string.h>
#include "cfl.h"
#include "cfl_page.h"
#include "cfl_table.h"
#include "cfl_row.h"

CflPageMaker*
CflPageMaker::Create()
{
  CflPageMaker *maker = new CflPageMaker();
  maker->buffer_ = (uint8_t*)malloc(CFL_PAGE_SIZE);
  if (maker->buffer_ == NULL)
  {
    delete maker;
    return NULL;
  }
  maker->Reset();

  return maker;
}

int
CflPageMaker::Destroy(CflPageMaker *maker)
{
  DBUG_ASSERT(maker != NULL);
  DBUG_ASSERT(maker->buffer_ != NULL);
  free(maker->buffer_);
  delete maker;
  return 0;
}

void
CflPageMaker::AddRow(void *row, uint16_t row_size)
{
  //将row写入缓冲区。拷贝行数据，写入行的偏移
  memcpy(row_offset_, row, row_size);
  cfl_page_write_row_offset(pos_offset_, row_pos_);

  rows_counter_++;
  row_pos_ += row_size;
  row_offset_ += row_size;
  pos_offset_ -= CFL_PAGE_ROW_POS_SIZE;
  DBUG_ASSERT(row_offset_ <= pos_offset_);
}

void
CflPageMaker::Flush(CflStorage *storage, cfl_dti_t key)
{
  //flush表示数据完成填写，进行行数据的上一级（up level）综合信息的填写
  cfl_page_write_row_count(buffer_, rows_counter_);
  //在当前的row_offset_写入row_pos_，确保通过row_pos_的计算得到行长度。
  cfl_page_write_row_offset(pos_offset_, row_pos_);
  //写入存储对象
  storage->WritePage(buffer_, rows_counter_, key);

  Reset();
}




/********************CflPageManager**********************/
CflPagePool *CflPageManager::pools_[CflPageMaxPool];

uint8_t CflPageManager::pools_number_ = 0;

bool CflPageManager::initialized_ = false;

uint8_t CflPageManager::curr_pool_ = 0;

int
CflPageManager::Initialize(int pools_number, int pool_size)
{
  CflPagePool *pool = NULL;
  bool fail = false;

  int id = 0;
  for (id = 0; id < pools_number; id++)
  {
    pool = CflPagePool::Create(pool_size, (uint8_t)id);
    if (pool == NULL)
    {
      fail = true;
      break;
    }
    
    pools_[id] = pool;
  }

  if (fail)
  {
    //错误处理，释放已有资源
    return -1;
  }

  pools_number_ = pools_number;
  curr_pool_ = 0;

  return 0;
}

int
CflPageManager::Deinitialize()
{
  int id = 0;
  CflPagePool *pool;
  int r = 0;
  int fail_count = 0;

  for (id = 0; id < pools_number_; id++)
  {
    pool = pools_[id];
    r = CflPagePool::Destroy(pool);
    if (r < 0)
      fail_count++;
  }

  if (fail_count > 0)
  {
    //出现错误
  }

  return 0;
}

CflPage*
CflPageManager::GetPage(CflStorage *storage, uint32_t nth_page)
{
  CflPage *page = NULL;
  CflPagePool *pool = NULL;

  uint8_t pool_id = 0;
  //to do : 获取一个page pool
  pool = pools_[pool_id];

  page = pool->GetPage(storage, nth_page);

  return page;
}

int
CflPageManager::PutPage(CflPage *page)
{
  uint8_t pool_id = page->pool_id();

  CflPagePool *pool = pools_[pool_id];

  pool->PutPage(page);

  return 0;
}

/**********************CflPagePool************************/
CflPagePool*
CflPagePool::Create(int size, uint8_t id)
{
  CflPagePool *pool = NULL;
  CflPage *page = NULL;

  pool = new CflPagePool();
  if (pool == NULL)
  {
  }

  //分配页面
  pool->pages_buffer_ = (uint8_t*)malloc(size * CFL_PAGE_SIZE);
  if (pool->pages_buffer_ == NULL)
  {
  }
  pool->pages_ = new CflPage[size];
  if (pool->pages_ == NULL)
  {
  }
  for (int i = 0; i < size; i++)
  {
    //初始化页对象
    page = pool->pages_ + i;
    page->pool_id_ = id;
    page->page_ = pool->pages_buffer_ + i * CFL_PAGE_SIZE;
    pool->free_pages_.push_back(page);
  }

  mysql_mutex_init(NULL, &pool->mutex_, MY_MUTEX_INIT_FAST);

  pool->id_ = id;
  return pool;
}

int
CflPagePool::Destroy(CflPagePool *pool)
{
  return 0;
}

CflPage*
CflPagePool::GetPage(CflStorage *storage, uint32_t nth_page)
{
  CflPage *page = NULL;
  int r = 0;

  //获取内存页对象
  page = Dequeue();
  //读取文件内容
  r = storage->ReadPage(page->page(), CFL_PAGE_SIZE, nth_page);
  if (r <= 0)
  {
    Enqueue(page);
    return NULL;
  }

  return page;
}

int
CflPagePool::PutPage(CflPage *page)
{
  DBUG_ASSERT(page->pool_id() == id_);
  Enqueue(page);
  return 0;
}

CflPage*
CflPagePool::Dequeue()
{
  CflPage *page = NULL;

  mysql_mutex_lock(&mutex_);
  if (free_pages_.size() > 0)
  {
    page = free_pages_.front();
    free_pages_.pop_front();
  }

  mysql_mutex_unlock(&mutex_);

  return page;
}

int
CflPagePool::Enqueue(CflPage *page)
{
  mysql_mutex_lock(&mutex_);

  free_pages_.push_back(page);

  mysql_mutex_unlock(&mutex_);

  return 0;
}

bool
cfl_page_locate_row(void *page, Field ** fields
                    , cfl_dti_t key , uint32_t *row_no)
{
  //to do
  uint32_t row_count;
  uint32_t low, high;
  uint32_t mid;

  *row_no = CFL_LOCATE_ROW_NULL;

  row_count = cfl_page_read_row_count(page);
  low = 0; high = row_count;
  mid = (low + high) / 2;

  cfl_dti_t row_key;
  bool found = false;
  uint8_t *row = NULL;
  row_key = cfl_row_get_key_data(fields, row);
  while (low + 1 < high)
  {
    if (row_key > key)
    {
      high = mid;
    }
    else if (row_key < key)
    {
      low = mid;
    }
    else
    {
      found = true;
      break;
    }
  }

  if (found)
  {
    *row_no = mid;
  }
  else
  {
    *row_no = high;
  }

  return found;
}
