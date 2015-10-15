#include "cfl_index.h"

uint32_t
CflIndex::Locate(cfl_dti_t key)
{
  //使用二分法进行定位
  uint32_t low, mid, high;
  uint32_t index_node_count = 0;
  bool found = false;
  cfl_dti_t index_key;

  low = 0;
  high = index_node_count + 1;
  mid = (low + high) / 2 + 1;
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

  if (found)
  {
    return mid;
  }

  return low;
}


