#ifndef _CFL_DATA_H_
#define _CFL_DATA_H_

#include <stdint.h>

/*
数据存储对象
数据部分只保存数据页，编号从0开始，0表示第一个数据页
页面的格式为
page magic head
page header
rows data
rows index
page magic tail
*/
class CflData
{
public :
  //
  void WritePage(uint32_t page_no, void *page, uint32_t page_size);

private :
  //文件
};

#endif //_CFL_DATA_H_
