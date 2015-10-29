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
  cfl_index_file_name(index_file_name, sizeof(index_file_name), name);

  int r = cf_create(index_file_name);
  if (r < 0)
  {
    return -1;
  }

  return 0;
}

int
CflIndex::DestroyIndexStorage(const char *name)
{
  char index_file_name[256];
  cfl_index_file_name(index_file_name, sizeof(index_file_name), name);
  int r = remove(index_file_name);
  if (r < 0)
    return -1;
  return 0;
}

CflIndex*
CflIndex::Create(const char *name)
{
  CflIndex * index = new CflIndex();
  char index_file_name[256];

  cfl_index_file_name(index_file_name, sizeof(index_file_name), name);
  int r = cf_open(index_file_name, &index->cf_file_);
  if (r < 0)
  {
    delete index;
    return NULL;
  }

  return index;
}

int
CflIndex::Destroy(CflIndex *index)
{
  int r = cf_close(&index->cf_file_);

  delete index;
  if (r < 0)
    return -1;
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


