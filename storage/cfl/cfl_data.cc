#include <string.h>
#include "cfl_data.h"

int
CflData::CreateDataStorage(const char *name)
{
  char data_file_name[256];

  strcpy(data_file_name, name);
  strcat(data_file_name, CFL_DATA_FILE_SUFFIX);
  
  FILE *f = fopen(data_file_name, "w+");

  if (f == NULL)
  {
    return -1;
  }

  fclose(f);
  return 0;
}
