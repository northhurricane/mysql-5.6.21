#ifndef _CFL_INDEX_H_
#define _CFL_INDEX_H_

#include <stdio.h>
#include "cfl_dt.h"
#include "cfl_endian.h"

/*
索引的存储对象
索引结构
index head(512 bytes)
index node:
*/
/*
index head的结构
index node number:32bit，用于记录当前index node的个数
*/
/*
index node:64bit，对应的数据页的最小时间节点
*/

#define CFL_INDEX_HEAD (0)
#define CFL_INDEX_NODE_COUNT (CFL_INDEX_HEAD)

#define CFL_INDEX_HEAD_FIX_SIZE (512)
#define CFL_INDEX_HEAD_TAIL (CFL_INDEX_HEAD_FIX_SIZE)


class CflIndex
{
public :
  /*定位key所在的页，可能存在多个页面的key相同，定位的是第一个*/
  uint32_t Locate(cfl_dti_t key);
  uint32_t ReadNodeCount()
  {
    uint8_t *node_count_pos = buffer_ + CFL_INDEX_NODE_COUNT;
    return endian_read_uint32(node_count_pos);
  }
  void WriteNodeCount(uint32_t node_count)
  {
    uint8_t *node_count_pos = buffer_ + CFL_INDEX_NODE_COUNT;
    endian_write_uint32(node_count_pos, node_count);
  }

private :
  /*
    nth:0-based
  */
  cfl_dti_t ReadNthIndexNode(uint32_t nth)
  {
    uint8_t *nth_node = buffer_ + CFL_INDEX_HEAD_TAIL
                               + nth * CFL_DTI_STORAGE_SIZE;
    return endian_read_uint64(nth_node);
  }
  //操作的文件
  FILE *index_file_;

  //缓冲区，作为index文件的镜像，便于快速读取
  uint8_t *buffer_;
  uint32_t *buffer_size_;
};

#endif //_CFL_INDEX_H_

