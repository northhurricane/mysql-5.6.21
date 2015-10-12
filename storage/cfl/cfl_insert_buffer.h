#ifndef _CFL_INSERT_BUFFER_H_
#define _CFL_INSERT_BUFFER_H_

/*
针对每个cfl表，有一个插入缓冲区，用于进行快速插入
该对象多线程下操作不安全
*/

#include <stdint.h>
#include <vector>
#include "cfl_dt.h"

using namespace std;

class CflTable;

/*
  用于在insert buffer中排序
*/
struct cfl_veckey_struct
{
  cfl_dti_t key;
  void *row;
};
typedef struct cfl_veckey_struct cfl_veckey_t;

class CflInsertBuffer
{
public :
  /*
    parameter:
      key:转换为ctrip fast log的时间key
      row:转换为ctrip fast log格式的row
      row_size:行数据的长度
    return:
      缓存中的的row所使用的空间
   */
  uint32_t Insert(cfl_dti_t key, void *row, uint16_t row_size);
  /*
    将buffer中的数据刷入磁盘
   */
  void FlushBuffer();
  /*
    获取缓存中row的个数
   */
  uint32_t GetRowsCount() { return sorted_eles_.size(); }
  uint32_t GetRowsSize() { return offset_; }

private :
  uint32_t Locate(cfl_dti_t key);
  uint32_t AddRow(uint32_t pos, cfl_dti_t key, void *row, uint16_t row_size);

private :
  //缓冲区，数据将顺序的写入缓冲区
  void *buffer_;
  uint32_t buffer_size_;
  uint32_t offset_; //当前写入位置

  //写入磁盘的最大的key。大于该key值的记录将被无法插入
  cfl_dti_t max_logged_key_;
  //当前排序的数据索引
  vector<cfl_veckey_t> sorted_eles_;
};


#endif //_CFL_INSERT_BUFFER_H_
