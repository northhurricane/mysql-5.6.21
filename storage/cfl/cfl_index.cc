#include "cfl_index.h"

int
cfl_index_file_name(char *buffer, uint32_t buffer_size, const char *name)
{
  char *index_file_name = (char*)buffer;

  strcpy(index_file_name, name);
  strcat(index_file_name, CFL_INDEX_FILE_SUFFIX);

  return 0;
}


int
CflIndex::CreateIndexStorage(const char *name)
{
  char index_file_name[256];

  strcpy(index_file_name, name);
  strcat(index_file_name, CFL_INDEX_FILE_SUFFIX);
  
  FILE *f = fopen(index_file_name, "w+");

  if (f == NULL)
  {
    return -1;
  }

  fclose(f);
  return 0;
}

int
CflIndex::DestroyIndexStorage(const char *name)
{
  char index_file_name[256];
  cfl_index_file_name(index_file_name, sizeof(index_file_name), name);
  int r = remove(index_file_name);
  return 0;
}

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


