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
  //
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
    //找到和key相等的页面索引
    //根据key_cmp确定页面
    page_no = mid;
    switch(key_cmp)
    {
    case KEY_EQUAL:
    case KEY_GE:
    case KEY_L:
    {
      //定位第一个和key相等的page
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
    {
      //定位第一个大于key的page
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
      if (index_key == key)
      {
        //未找到相应的key
        DBUG_ASSERT(page_no == (index_node_count + 1));
        page_no = CFL_LOCATE_PAGE_NULL;
      }
      break;
    }
    case KEY_LE:
    {
      //定位最后一个和key相等的page
      uint32_t page_no_prev = page_no;
      page_no++;
      while (page_no < index_node_count + 1)
      {
        index_key = ReadNthIndexNode(page_no - 1);
        if (index_key > key)
        {
          break;
        }

        page_no_prev = page_no;
        page_no++;
      }
      page_no = page_no_prev;
      break;
    }
    default:
      DBUG_ASSERT(false);
    }

    return page_no;
  }

  //未找到和key相等的页面索引，此时key的值在low和high之间
  switch(key_cmp)
  {
  case KEY_EQUAL:
  case KEY_GE:
  case KEY_G:
  {
    //所需要定位的记录在high所指向的page
    //如果high等于index_node_count + 1则说明没有合适的page页
    if (high == (index_node_count + 1))
      page_no = CFL_LOCATE_PAGE_NULL;
    else
      page_no = high;
    break;
  }
  case KEY_LE:
  case KEY_L:
  {
    //所需要定位的记录在high所指向的page
    //如果high等于index_node_count + 1则所定位的记录在low的page中
    if (high == (index_node_count + 1))
      page_no = high - 1;
    else
      page_no = high;
    break;
  }
  default:
    DBUG_ASSERT(false);
  }

  return page_no;
}


