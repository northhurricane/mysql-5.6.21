#include <string.h>
#include "cfl_insert_buffer.h"


CflInsertBuffer*
CflInsertBuffer::Create()
{
  return NULL;
}

void
CflInsertBuffer::Destroy(CflInsertBuffer *insert_buffer)
{
}

uint32_t
CflInsertBuffer::Insert(cfl_dti_t key, void *row, uint16_t row_size)
{
  uint32_t pos = 0;

  //to do : 判断记录是否可以插入
  //定位
  pos = Locate(key);

  //拷贝到指定位置
  memcpy(buffer_ + offset_, row, row_size);
  offset_ += row_size;

  //key加入到vector中
  cfl_veckey_t veckey;
  veckey.key = key;
  sorted_eles_.insert(sorted_eles_.begin() + pos, veckey);

  return offset_;
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




