#include "cfl_index.h"

int
cfl_index_file_name(char *buffer, uint32_t buffer_size, const char *name)
{
  char *index_file_name = (char*)buffer;

  strcpy(index_file_name, name);
  strcat(index_file_name, CFL_INDEX_FILE_SUFFIX);

  return 0;
}

int
CflIndex::AddPage(cfl_dti_t dti)
{
  //写入index node
  WriteNthIndexNode(node_count_, dti);
  IncrNodeCount();
  //写入头数据
  WriteHead();
  return 0;
}

int
CflIndex::WriteHead()
{
  int r = cf_write(&cf_file_, 0, buffer_, CFL_INDEX_HEAD_FIX_SIZE);
  if (r < 0)
    return -1;
  return 0;
}

void
CflIndex::IncrNodeCount()
{
  node_count_++;
  WriteNodeCount(node_count_);
}

int
CflIndex::CreateIndexStorage(const char *name)
{
  char index_file_name[256];
  cfl_index_file_name(index_file_name, sizeof(index_file_name), name);

  int r = cf_create(index_file_name);
  if (r < 0)
  {
    return -1;
  }

  return 0;
}

int
CflIndex::DestroyIndexStorage(const char *name)
{
  char index_file_name[256];
  cfl_index_file_name(index_file_name, sizeof(index_file_name), name);
  int r = remove(index_file_name);
  if (r < 0)
    return -1;
  return 0;
}

CflIndex*
CflIndex::Create(const char *name)
{
  CflIndex * index = new CflIndex();
  char index_file_name[256];

  cfl_index_file_name(index_file_name, sizeof(index_file_name), name);
  int r = cf_open(index_file_name, &index->cf_file_);
  if (r < 0)
  {
    printf(strerror(errno));
    delete index;
    return NULL;
  }
  index->buffer_ = (uint8_t*)malloc(CFL_INDEX_BUFFER_INIT_SIZE);
  index->buffer_size_ = CFL_INDEX_BUFFER_INIT_SIZE;

  r = cf_read(&index->cf_file_, 0, index->buffer_
              , CFL_INDEX_BUFFER_INIT_SIZE, CFL_INDEX_BUFFER_INIT_SIZE);
  index->node_count_ = index->ReadNodeCount();

  return index;
}

int
CflIndex::Destroy(CflIndex *index)
{
  int r = cf_close(&index->cf_file_);
  if (index->buffer_ != NULL)
    free(index->buffer_);

  delete index;
  if (r < 0)
    return -1;
  return 0;
}

/*
页面定位说明。
情况EQUAL:
  定位的记录是最前一个等于搜索key的记录。
  定位页面的key等于或者大于搜索key，定位页面的前一个页面的key必定小于搜索key。
  如果定位到的页面是虚拟最大页面，则说明没有满足条件的记录
情况GE:
  定位的记录是情况E的记录和气候的记录。
  定位页面和情况EQUAL相同
  如果定位到的页面是虚拟最大页面，则说明没有满足条件的记录
情况G:
  定位的记录是大于搜索key的最小记录。
  定位页面的key大于搜索key，定位页面的前一个页面的key必定等于或者小于搜索key
  如果定位到的页面是虚拟最大页面，则说明没有满足条件的记录
情况LE:
  定位的记录是最后一个等于搜索key的记录
  定位页面的key大于搜索key，定位页面的前一个页面的key必定等于或者小于搜索key
  在此处，不存在没有满足条件的记录
情况L:
  定位的记录是小于搜索key的最大记录
  定位页面的key等于或者大于搜索key，定位页面的前一个页面的key必定小于搜索key
  在此处，不存在没有满足条件的记录
*/
uint32_t
CflIndex::LocatePage(cfl_dti_t key, cfl_key_cmp key_cmp)
{
  //使用二分法进行定位
  uint32_t low, mid, high;
  uint32_t index_node_count = node_count_;
  bool found = false;
  cfl_dti_t index_key;

  //如果没有数据内容
  if (index_node_count == 0)
    return CFL_LOCATE_PAGE_NULL;

  low = 0;//虚拟最小记录
  high = index_node_count + 1; //虚拟最大记录
  mid = (low + high) / 2; //中间记录，
  //定位
  while (low + 1 < high)
  {
    index_key = ReadNthIndexNode(mid - 1);
    if (index_key > key)
    {
      high = mid;
    }
    else if (index_key < key)
    {
      low = mid;
    }
    else
    {
      found = true;
      break;
    }
  }

  uint32_t page_no = CFL_LOCATE_PAGE_NULL; //1-based
  if (found)
  {
    //1、找到和key相等的页面索引
    //2、当定位到某个页面的key和搜索key相等，则EQUAL/GE情况必然存在满足条件的
    //记录。只有G情况存在不满足条件的记录
    page_no = mid;
    switch(key_cmp)
    {
    case KEY_EQUAL:
    case KEY_GE:
    case KEY_L:
    {
      //定位第一个和key相等的page
      //原因1，对于EQUAL/GE来说，这是毫无疑问的
      //原因2，对于L来说，满足条件的记录只能在，第一个和参数key相等的页面，和前一个页面
      uint32_t page_no_next = page_no;
      page_no--;
      while (page_no > 0)
      {
        index_key = ReadNthIndexNode(page_no - 1);
        if (index_key < key)
        {
          break;
        }

        page_no_next = page_no;
        page_no--;
      }
      page_no = page_no_next;
      break;
    }
    case KEY_G:
    case KEY_LE:
    {
      //定位第一个大于key的page
      //原因1，对于G来说，所有页面的key值等于参数key值得页面必然是不符合要求，
      //也就是，这些页面的时间标记都小于参数key，所以必须是大于该key的
      //原因2，对于LE来说，由于时间字段不是唯一标识。所以，最后一个页面的key值
      //等于搜索key的，在其后页面的记录，有可能等于搜索key
      page_no++;
      while (page_no < (index_node_count + 1))
      {
        index_key = ReadNthIndexNode(page_no - 1);
        if (index_key > key)
        {
          break;
        }
        page_no++;
      }
      if (page_no == (index_node_count + 1))
      {
        //只有虚拟最大页面的key大于搜索key
        if (key_cmp == KEY_G)
        {
          //所有记录都不满足条件
          page_no = CFL_LOCATE_PAGE_NULL;
        }
        else
        {
          //所有记录都满足条件
          DBUG_ASSERT(key_cmp == KEY_LE);
          page_no--;
        }
      }
      break;
    }
    default:
      DBUG_ASSERT(false);
    }

    return page_no;
  }

  //未找到和key相等的页面索引，此时key的值在low和high之间
  //也就是参数key的值必然大于low所指页面的记录
  switch(key_cmp)
  {
  case KEY_EQUAL:
  case KEY_GE:
  case KEY_G:
  {
    //如果high等于index_node_count + 1则说明最大页的所有记录都小于搜索key
    if (high == (index_node_count + 1))
      page_no = CFL_LOCATE_PAGE_NULL;
    else
      page_no = high;
    break;
  }
  case KEY_LE:
  case KEY_L:
  {
    //所有小于high的页面都是满足条件的页面，high所指向的页面可能部分满足条件
    //如果high等于index_node_count + 1则所定位的记录在low的page中
    if (high == (index_node_count + 1))
      page_no = index_node_count;
    else
      page_no = high;
    break;
  }
  default:
    DBUG_ASSERT(false);
  }

  return page_no;
}


