#ifndef _CFL_BUFFER_H_
#define _CFL_BUFFER_H_

/*
说明
1、用于进行clf page的缓冲区，避免频繁分配释放
2、可动态配置，增多或者减少缓冲区
*/
#include <list>
#include <stdint.h>

using namespace std;

class CflBuffer;

/*
buffer管理，管理多个
*/
class CflBufferManager
{
public :
  static void Initialize();

private :
  list<CflBuffer*> buffers_;


  static CflBufferManager *instance_;
};

class CflBuffer
{
private:
  
  void *buffer_;
};




#endif //_CFL_BUFFER_H_
