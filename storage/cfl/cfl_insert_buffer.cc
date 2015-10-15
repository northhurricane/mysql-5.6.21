#include <string.h>
#include <stdlib.h>
#include "cfl.h"
#include "cfl_insert_buffer.h"
#include "cfl_page.h"
#include "cfl_table.h"

CflInsertBuffer*
CflInsertBuffer::Create()
{
  //生成对象
  CflInsertBuffer *insert_buffer = new CflInsertBuffer();

  //初始化对象，分配相应资源
  insert_buffer->Initialize();

  return insert_buffer;
}

void
CflInsertBuffer::Destroy(CflInsertBuffer *insert_buffer)
{
  insert_buffer->Deinitialize();
  delete insert_buffer;
}

uint32_t
CflInsertBuffer::Insert(cfl_dti_t key, void *row, uint16_t row_size)
{
  uint32_t pos = 0;

  //to do : 判断记录是否可以插入
  //定位
  pos = Locate(key);

  cfl_veckey_t veckey;
  //拷贝到指定位置
  memcpy(buffer_ + offset_, row, row_size);
  veckey.row_pos = offset_;
  offset_ += row_size;

  //key加入到vector中
  veckey.key = key;
  sorted_eles_.insert(sorted_eles_.begin() + pos, veckey);

  veckey.row_size = row_size;

  return offset_;
}

int
CflInsertBuffer::Flush(CflPageMaker *maker)
{
  uint32_t max_row = sorted_eles_.size() - 1;

  for (uint32_t i = 1; i < max_row; i++)
  {
    maker->AddRow(buffer_ + sorted_eles_[i].row_pos
                    , sorted_eles_[i].row_size);
  }

  maker->Flush(0);

  return 0;
}

bool
CflInsertBuffer::Initialize()
{
  offset_ = 0;
  //buffer_ = NULL;
  buffer_ = (uint8_t*)malloc(CFL_PAGE_SIZE);
  if (buffer_ == NULL)
    goto fail;
  buffer_size_ = 0;

  return true;

fail :
  return false;
}

bool
CflInsertBuffer::Deinitialize()
{
  DBUG_ASSERT(buffer_ != NULL);

  free(buffer_);

  return true;
}

bool
CflInsertBuffer::Reset()
{
  offset_ = 0;

  return true;
}

uint32_t
CflInsertBuffer::Locate(cfl_dti_t key)
{
  //二分查找进行定位，insert buffer的二分查找是包含伪记录的，所以当有10个记录时，则在vector中有12个元素。
  uint32_t high, mid, low;
  high = sorted_eles_.size() - 1;
  low = 0;
  mid = (low + high) / 2 + 1;
  bool found = false;
  cfl_veckey_t mid_ele;

  while (low + 1 < high)
  {
    mid_ele = sorted_eles_[mid];
    if (mid_ele.key > key)
    {
      high = mid;
    }
    else if (mid_ele.key < key)
    {
      low = mid;
    }
    else
    {
      break;
    }
    mid = (low + high) / 2 + 1;
  }

  if (found)
  {
    return mid + 1;
  }

  return high;
}




