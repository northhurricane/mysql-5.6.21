#include "cfl_table.h"
#include "cfl_index.h"
#include "cfl_data.h"
#include "cfl_insert_buffer.h"
#include "cfl_page.h"

#include <map>
using namespace std;

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

  int r = storage->Initialize(name);
  if (r < 0)
    return NULL;

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
  if (index_ == NULL)
    return -1;

  uint32_t index_node_number = index_->ReadIndexNodeNumber();
  data_ = CflData::Create(name, index_node_number);
  if (data_ == NULL)
    return -1;

  return 0;

  //to do : fail process
}

int
CflStorage::Deinitialize()
{
  return 0;
}

bool
CflStorage::LocateRow(cfl_dti_t key, enum cfl_key_cmp keycmp
                      , cfl_cursor_t &cursor, Field **fields)
{
  cursor.page_no = CFL_LOCATE_PAGE_NULL;
  cursor.row_no = CFL_LOCATE_ROW_NULL;

  //根据key和keycmp进行相关的定位
  uint32_t page_no;  //0-based. for LocatePage and cursor
  uint32_t row_no;   //0-based. for cursor

  page_no = index_->LocatePage(key, keycmp);
  if (page_no == CFL_LOCATE_PAGE_NULL)
  {
    //未找到满足条件的数据
    return false;
  }

  CflPage *page = NULL;
  uint8_t *page_data = NULL;
  page = CflPageManager::GetPage(this, page_no);
  page_data = (uint8_t*)page->page();

  //在页中定位行
  bool found = false;   //是否在页面内定位到数据
  uint32_t row_no_found = 0; //1-based。便于等值时代码编写，使用该计数。基于
                             //该变量的变量，row_no_curr等也是如此
  found = cfl_page_locate_row(page_data, fields, key, &row_no_found);

  //便于程序表达和书写，row_no_curr/row_no_prev/row_no_next，采用1-base方式
  uint32_t row_no_curr = row_no_found + 1; //1-based。
  uint32_t row_no_prev = CFL_LOCATE_ROW_NULL; //1-based
  uint32_t row_no_next = CFL_LOCATE_ROW_NULL; //1-based

  uint32_t row_count;        //当前页的行数
  row_count = cfl_page_read_row_count(page_data);
  if (found)
  {
    cfl_dti_t row_key;
    switch (keycmp)
    {
    case KEY_EQUAL:
    case KEY_GE:
    {
      //向前遍历，定位等于该key的第一个记录
      row_no_next = row_no_curr;
      row_no_curr--;
      //根据row_no_curr获取当前row的key
      while (row_no_curr > 0)
      {
        row_key = cfl_page_nth_row_key(page_data, row_no_curr - 1, fields);
        if (row_key < key)
          break;
        row_no_next = row_no_curr;
        row_no_curr--;
      }
      row_no = row_no_next - 1;
      break;
    }
    case KEY_G:
    {
      //当前页面key必然是大于索引key的最小页，在该页面必然存在满足条件的记录
      //向后遍历，定位大于该key的第一个记录，如果超过最大记录，则必定在下一页中
      //定位记录
      row_no_prev = row_no_curr;
      row_no_curr++;
      while (row_no_curr <= row_count)
      {
        row_key = cfl_page_nth_row_key(page_data, row_no_curr - 1, fields);
        if (row_key > key)
          break;
        row_no_prev = row_no_curr;
        row_no_curr++;
      }
      //对于KEY_G的情况，在页面内找到相等的记录，该页的最大key必然大于搜索key
      DBUG_ASSERT(row_no_curr <= row_count);
      row_no = row_no_curr - 1;
      break;
    }
    case KEY_LE:
    {
      //当前页面的key必然是包含索引key值的最大页面
      //向后遍历，定位等于该key的记录最后一条记录
      row_no_prev = row_no_curr;
      row_no_curr++;
      while (row_no_curr <= row_count)
      {
        row_key = cfl_page_nth_row_key(page_data, row_no_curr - 1, fields);
        if (row_key > key)
          break;
        row_no_prev = row_no_curr;
        row_no_curr++;
      }
      row_no = row_no_prev - 1;
      break;
    }
    case KEY_L:
    {
      //向前遍历，定位小于该key的第一个记录。
      //如果不在当前页，则必然在前一页。如果前一页不存在，则无满足条件记录
      row_no_next = row_no_curr;
      row_no_curr--;
      while (row_no_curr > 0)
      {
        row_key = cfl_page_nth_row_key(page_data, row_no_curr - 1, fields);
        if (row_key < key)
          break;
        row_no_next = row_no_curr;
        row_no_curr--;
      }
      if (row_no_curr == 0)
      {
        if  (page_no == 0)
          goto not_found;
        //获取前一页
        CflPageManager::PutPage(page);
        page_no--;
        page = CflPageManager::GetPage(this, page_no);
        page_data = (uint8_t*)page->page();
        row_count = cfl_page_read_row_count(page_data);
        row_no = row_count - 1;
      }
      row_no = row_no_curr - 1;
      break;
    }
    default:
      DBUG_ASSERT(false);
    }
  }
  else
  {
    //为在页面内定位到与搜索key相等的记录，说明在页面搜索时就不存在等值情况
    switch (keycmp)
    {
    case KEY_EQUAL:
    {
      //等值定位，报告未找到数据
      goto not_found;
    }
    case KEY_GE:
    case KEY_G:
    {
      //此时搜索key必然小于当前页的最大key，待定位的记录必然在该页面内
      //也就是high所指向的记录
      row_no = row_no_found;
      break;
    }
    case KEY_LE:
    case KEY_L:
    {
      //由于不存在等值的情况
      //此时该key必然小于当前页的最大key，但大于前一页的最大key
      //所以，待定位的记录必然存在当前页或者前一页
      //如果不在当前页，在必定在前一页
      //如果不存在前一页，在说明不存在要查询的记录
      if (row_no_found == 0)
      {
        //搜索key小于该页面所有记录的key，所要定位的记录在前一页
        CflPageManager::PutPage(page);
        if (page_no == 0)
        {
          //如果不存在前一页，在说明不存在要查询的记录
          row_no = CFL_LOCATE_ROW_NULL;
          goto not_found;
        }
        else
        {
          page_no--;
          page = CflPageManager::GetPage(this, page_no);
          page_data = (uint8_t*)page->page();
          row_count = cfl_page_read_row_count(page_data);
          row_no = row_count - 1;
        }
      }
      else
      {
        //在当前页
        row_no = row_no_found - 1;
      }
    }
    default:
      DBUG_ASSERT(false);
    }
  }

  cfl_cursor_page_no_set(cursor, page_no);
  cfl_cursor_row_no_set(cursor, row_no);
  cfl_cursor_page_set(cursor, page);

  return true;

not_found:
  //没有满足条件的记录
  CflPageManager::PutPage(page);
  return false;
  
}

/*
CflTableInstanceManager
*/
/*
用于管理CflTable的实例，在运行过程中，只能有一个指向文件句柄的CflTable的实例存在
*/
typedef map<string , CflTable*>  table_map_t;
typedef table_map_t::iterator  table_map_it_t;

class CflTableInstanceManager
{
public :
  CflTable* Create(const char *name);
  int Destroy(CflTable *table);
  CflTableInstanceManager();
  ~CflTableInstanceManager();

private :
  mysql_mutex_t table_map_mutex_;
  table_map_t table_map_;
};

CflTableInstanceManager::CflTableInstanceManager()
{
  mysql_mutex_init(NULL, &table_map_mutex_, MY_MUTEX_INIT_FAST);
}

CflTableInstanceManager::~CflTableInstanceManager()
{
  mysql_mutex_destroy(&table_map_mutex_);
}

static CflTableInstanceManager table_manager;

CflTable* CflTableInstanceManager::Create(const char *name)
{
  string str_name = name;
  CflTable *table = NULL;

  mysql_mutex_lock(&table_map_mutex_);
  table_map_it_t it = table_map_.find(str_name);
  if (it == table_map_.end())
  {
    table = CflTable::CreateInstance(name);
    if (table != NULL)
    {
      table_map_[str_name] = table;
      table->IncRefCount();
    }
  }
  else
  {
    table = it->second;
    table->IncRefCount();
    //table = table_map_[str_name];
  }
  mysql_mutex_unlock(&table_map_mutex_);

  return table;
}

int CflTableInstanceManager::Destroy(CflTable *table)
{
  int r = 0;
  mysql_mutex_lock(&table_map_mutex_);
  const string &str_name = table->GetTableName();
  table_map_it_t it = table_map_.find(str_name);
  DBUG_ASSERT(it != table_map_.end());
  /*
    CflTable *table_find = it->second;
    DBUG_ASSERT(table_find != NULL);
  */
  uint32_t ref_count = table->DecRefCount();
  if (ref_count == 0)
  {
    table_map_.erase(it);
    r = CflTable::DestroyInstance(table);
  }
  mysql_mutex_unlock(&table_map_mutex_);
  return r;
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
  return table_manager.Create(name);
}

int
CflTable::Destroy(CflTable *table)
{
  DBUG_ASSERT(table != NULL);
  return table_manager.Destroy(table);
}

CflTable*
CflTable::CreateInstance(const char *name)
{
  CflTable *table = new CflTable();
  if (table == NULL)
    return NULL;

  table->table_name_ = name;
  int r = table->Initialize(name);
  if (r < 0)
  {
    delete table;
    return NULL;
  }

  return table;
}

int
CflTable::DestroyInstance(CflTable *table)
{
  DBUG_ASSERT(table != NULL);

  table->Deinitialize();
  delete table;

  return 0;
}

/*CflTable*
CflTable::Open()
{
  return 0;
}

int
CflTable::Close()
{
  return 0;
}*/

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

int
CflTable::Truncate()
{
  //清理数据
  insert_buffer_->Clear();

  CflStorage::Close(storage_);
  CflStorage::DestroyStorage(table_name_.c_str());
  CflStorage::CreateStorage(table_name_.c_str());
  storage_ = CflStorage::Open(table_name_.c_str());

  return 0;
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
  /*if (total_size > 80)
    return true;*/

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

