#ifndef _CFL_INDEX_H_
#define _CFL_INDEX_H_


#include "cfl_dt.h"

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
class CflIndex
{
public :
  /*定位key所在的页，可能存在多个页面的key相同，定位的是第一个*/
  uint32_t Locate(cfl_dti_t key);

private :
  cfl_dti_t ReadNthIndexNode(uint32_t nth);
  //操作的文件
};

#endif //_CFL_INDEX_H_

