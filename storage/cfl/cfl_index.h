#ifndef _CFL_INDEX_H_
#define _CFL_INDEX_H_

#include <stdio.h>
#include "cfl.h"
#include "cfl_dt.h"
#include "cfl_endian.h"
#include "cfl_file.h"

/*
索引的存储对象
索引结构
index head(512 bytes)
index node:
*/
/*
index head的结构
index node number:32bit，用于记录当前index node的个数。也就是数据文件中的有效页面数
*/
/*
index node:64bit，对应的数据页的最大时间节点。
*/

#define CFL_INDEX_HEAD (0)
#define CFL_INDEX_NODE_COUNT (CFL_INDEX_HEAD)

#define CFL_INDEX_HEAD_FIX_SIZE (512)
#define CFL_INDEX_HEAD_TAIL (CFL_INDEX_HEAD_FIX_SIZE)

#define CFL_INDEX_FILE_SUFFIX ".cfli"

#define CFL_INDEX_BUFFER_INIT_SIZE (1024 * 1024)

class CflIndex
{
public :
  static int CreateIndexStorage(const char *name);
  static int DestroyIndexStorage(const char *name);
  static CflIndex *Create(const char *name);
  static int Destroy(CflIndex *index);

  /*
    定位key所在的页，可能存在多个页面的key相同，定位的是第一个
    返回值：1-based。
  */
  uint32_t LocatePage(cfl_dti_t key, enum cfl_key_cmp keycmp);
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
  int AddPage(cfl_dti_t dti);

  uint32_t ReadIndexNodeNumber()
  {
    return endian_read_uint32(buffer_ + CFL_INDEX_NODE_COUNT);
  }

private :
  /*
    nth:0-based
  */
  cfl_dti_t ReadNthIndexNode(uint32_t nth)
  {
    uint8_t *nth_node = buffer_ + CFL_INDEX_HEAD_TAIL
                               + nth * CFL_DTI_STORAGE_SIZE;
    return cfl_s2dti(nth_node);
    //    return endian_read_uint64(nth_node);
  }
  int WriteNthIndexNode(uint32_t nth, cfl_dti_t dti)
  {
    uint8_t *nth_node = buffer_ + CFL_INDEX_HEAD_TAIL
                               + nth * CFL_DTI_STORAGE_SIZE;
    cfl_dti2s(dti, nth_node);
    return 0;
  }

  //操作的文件
  cf_t cf_file_;

  //缓冲区，作为index文件的镜像，便于快速读取
  uint8_t *buffer_;
  uint32_t buffer_size_;

  uint32_t node_count_; //当前索引的

  int WriteHead();
  void IncrNodeCount() ;
};

#endif //_CFL_INDEX_H_

