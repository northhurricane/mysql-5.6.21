#include "cfl_insert_buffer.h"

uint32_t
CflInsertBuffer::Insert(cfl_dti_t key, void *row, uint16_t row_size)
{
  uint32_t pos = 0;

  //to do : 判断记录是否可以插入
  //to do : 进行插入保护
  //定位
  pos = Locate(key);
  //拷贝到指定位置

}

uint32_t
CflInsertBuffer::Locate(cfl_dti_t key)
{
  //二分查找进行定位
  return 0;
}

uint32_t
CflInsertBuffer::AddRow(uint32_t pos, cfl_dti_t key
                        , void *row, uint16_t row_size)
{
  //拷贝数据
  //插入索引
  return 0;
}



