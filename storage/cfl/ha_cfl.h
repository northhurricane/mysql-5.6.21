#ifndef _HA_CFL_H_
#define _HA_CFL_H_ 

/* Copyright (c) 2004, 2012, Oracle and/or its affiliates. All rights reserved.

  This program is free software; you can redistribute it and/or modify
  it under the terms of the GNU General Public License as published by
  the Free Software Foundation; version 2 of the License.

  This program is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU General Public License for more details.

  You should have received a copy of the GNU General Public License
  along with this program; if not, write to the Free Software
  Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

/** @file ha_cfl.h

    @brief
  The ha_cfl engine is a stubbed storage engine for example purposes only;
  it does nothing at this point. Its purpose is to provide a source
  code illustration of how to begin writing new storage engines; see also
  /storage/cfl/ha_cfl.cc.

    @note
  Please read ha_cfl.cc before reading this file.
  Reminder: The example storage engine implements all methods that are *required*
  to be implemented. For a full list of all methods that you can implement, see
  handler.h.

   @see
  /sql/handler.h and /storage/cfl/ha_cfl.cc
*/

#include "my_global.h"                   /* ulonglong */
#include "thr_lock.h"                    /* THR_LOCK, THR_LOCK_DATA */
#include "handler.h"                     /* handler */
#include "my_base.h"                     /* ha_rows */
#include <stdint.h>
#include "cfl.h"
#include "cfl_cursor.h"
#include "cfl_dt.h"

#define CFL_MAX_COLUMN_IN_KEY_DEFINATION (1)
#define CFL_MAX_INDEX_IN_TABLE_DEFINATION (1)
#define CFL_MAX_KEY_LENGTH (20)

class CflTable;
class CflInsertBuffer;

/** @brief
  Example_share is a class that will be shared among all open handlers.
  This example implements the minimum of what you will probably need.
*/
class Cfl_share : public Handler_share {
public:
  mysql_mutex_t mutex;
  THR_LOCK lock;
  Cfl_share();
  ~Cfl_share()
  {
    thr_lock_delete(&lock);
    mysql_mutex_destroy(&mutex);
  }
};

/*
  index search struct
*/
struct cfl_isearch_struct
{
  cfl_dti_t key;
  enum cfl_key_cmp key_cmp;  
};
typedef struct cfl_isearch_struct cfl_isearch_t;

/** @brief
  Class definition for the storage engine
*/
class ha_cfl: public handler
{
  THR_LOCK_DATA lock;      ///< MySQL lock
  Cfl_share *share;    ///< Shared lock info
  Cfl_share *get_share(); ///< Get the share

public:
  ha_cfl(handlerton *hton, TABLE_SHARE *table_arg);
  ~ha_cfl();

  /** @brief
    The name that will be used for display purposes.
   */
  const char *table_type() const { return "CFL"; }

  /** @brief
    The name of the index type that will be used for display.
    Don't implement this method unless you really have indexes.
   */
  const char *index_type(uint inx) { return "UNKNOWN"; }

  /** @brief
    The file extensions.
   */
  const char **bas_ext() const;

  /** @brief
    This is a list of flags that indicate what functionality the storage engine
    implements. The current table flags are documented in handler.h
  */
  ulonglong table_flags() const
  {
    /*
      We are saying that this engine is just statement capable to have
      an engine that can only handle statement-based logging. This is
      used in testing.
    */
    return HA_BINLOG_STMT_CAPABLE;
  }

  /** @brief
    This is a bitmap of flags that indicates how the storage engine
    implements indexes. The current index flags are documented in
    handler.h. If you do not implement indexes, just return zero here.

      @details
    part is the key part to check. First key part is 0.
    If all_parts is set, MySQL wants to know the flags for the combined
    index, up to and including 'part'.
  */
  ulong index_flags(uint inx, uint part, bool all_parts) const
  {
    //不应该出现该情况
    if (table_share->key_info[inx].algorithm == HA_KEY_ALG_FULLTEXT)
      return 0;

    return (HA_READ_NEXT | HA_READ_PREV | HA_READ_ORDER
            | HA_READ_RANGE | HA_KEYREAD_ONLY
            | HA_DO_INDEX_COND_PUSHDOWN);
    //    return 0;
  }

  /** @brief
    unireg.cc will call max_supported_record_length(), max_supported_keys(),
    max_supported_key_parts(), uint max_supported_key_length()
    to make sure that the storage engine can handle the data it is about to
    send. Return *real* limits of your storage engine here; MySQL will do
    min(your_limits, MySQL_limits) automatically.
   */
  uint max_supported_record_length() const { return HA_MAX_REC_LENGTH; }

  /** @brief
    unireg.cc will call this to make sure that the storage engine can handle
    the data it is about to send. Return *real* limits of your storage engine
    here; MySQL will do min(your_limits, MySQL_limits) automatically.

      @details
    There is no need to implement ..._key_... methods if your engine doesn't
    support indexes.
   */
  uint max_supported_keys() const
  { return CFL_MAX_INDEX_IN_TABLE_DEFINATION; }

  /** @brief
    unireg.cc will call this to make sure that the storage engine can handle
    the data it is about to send. Return *real* limits of your storage engine
    here; MySQL will do min(your_limits, MySQL_limits) automatically.

      @details
    There is no need to implement ..._key_... methods if your engine doesn't
    support indexes.
   */
  uint max_supported_key_parts() const
  {return CFL_MAX_COLUMN_IN_KEY_DEFINATION; }

  /** @brief
    unireg.cc will call this to make sure that the storage engine can handle
    the data it is about to send. Return *real* limits of your storage engine
    here; MySQL will do min(your_limits, MySQL_limits) automatically.

      @details
    There is no need to implement ..._key_... methods if your engine doesn't
    support indexes.
   */
  uint max_supported_key_length() const
  { return CFL_MAX_KEY_LENGTH; }

  /** @brief
    Called in test_quick_select to determine if indexes should be used.
  */
  virtual double scan_time() { return (double) (stats.records+stats.deleted) / 20.0+10; }

  /** @brief
    This method will never be called if you do not implement indexes.
  */
  virtual double read_time(uint, uint, ha_rows rows)
  { return (double) rows /  20.0+1; }

  /*
    Everything below are methods that we implement in ha_cfl.cc.

    Most of these methods are not obligatory, skip them and
    MySQL will treat them as not implemented
  */
  /** @brief
    We implement this in ha_cfl.cc; it's a required method.
  */
  int open(const char *name, int mode, uint test_if_locked);    // required

  /** @brief
    We implement this in ha_cfl.cc; it's a required method.
  */
  int close(void);                                              // required

  /** @brief
    We implement this in ha_cfl.cc. It's not an obligatory method;
    skip it and and MySQL will treat it as not implemented.
  */
  int write_row(uchar *buf);

  /** @brief
    We implement this in ha_cfl.cc. It's not an obligatory method;
    skip it and and MySQL will treat it as not implemented.
  */
  int update_row(const uchar *old_data, uchar *new_data);

  /** @brief
    We implement this in ha_cfl.cc. It's not an obligatory method;
    skip it and and MySQL will treat it as not implemented.
  */
  int delete_row(const uchar *buf);

  /** @brief
    We implement this in ha_cfl.cc. It's not an obligatory method;
    skip it and and MySQL will treat it as not implemented.
  */
  /*int index_read_map(uchar *buf, const uchar *key,
    key_part_map keypart_map, enum ha_rkey_function find_flag);*/

  /*索引查询*/
  /** @brief
      使用索引查询的初始化动作在此
      参数：
        index:索引查询所使用的索引编号
        sorted:待研究
  */
  int index_init(uint index, bool sorted);
  /** @brief
      完成索引查询后的结束动作在此
  */
  int index_end();
  /** @brief
      进行索引的读取。
      参数
        buf
        key:进行查询的key数据
        key_len:数据的长度
        find_flag:进行索引定位。HA_READ_AFTER_KEY，表示在定位到大于key数据的最小记录
  */
  int index_read(uchar * buf, const uchar * key, uint key_len,
                 enum ha_rkey_function find_flag);
  /** @brief
      Used to read forward through the index.
  */
  int index_next(uchar *buf);
  /** @brief
      Used to read backwards through the index.
  */
  int index_prev(uchar *buf);

  /** @brief
    We implement this in ha_cfl.cc. It's not an obligatory method;
    skip it and and MySQL will treat it as not implemented.
  */
  int index_first(uchar *buf);

  /** @brief
    We implement this in ha_cfl.cc. It's not an obligatory method;
    skip it and and MySQL will treat it as not implemented.
  */
  int index_last(uchar *buf);

  /** @brief
    Unlike index_init(), rnd_init() can be called two consecutive times
    without rnd_end() in between (it only makes sense if scan=1). In this
    case, the second call should prepare for the new table scan (e.g if
    rnd_init() allocates the cursor, the second call should position the
    cursor to the start of the table; no need to deallocate and allocate
    it again. This is a required method.
  */
  int rnd_init(bool scan);                                      //required
  int rnd_end();
  int rnd_next(uchar *buf);                                     ///< required
  int rnd_pos(uchar *buf, uchar *pos);                          ///< required
  void position(const uchar *record);                           ///< required
  int info(uint);                                               ///< required
  int extra(enum ha_extra_function operation);
  int external_lock(THD *thd, int lock_type);                   ///< required
  int delete_all_rows(void);
  int truncate();
  ha_rows records_in_range(uint inx, key_range *min_key,
                           key_range *max_key);
  int delete_table(const char *from);
  int rename_table(const char * from, const char * to);
  int create(const char *name, TABLE *form,
             HA_CREATE_INFO *create_info);                      ///< required

  THR_LOCK_DATA **store_lock(THD *thd, THR_LOCK_DATA **to,
                             enum thr_lock_type lock_type);     ///< required


  int multi_range_read_init(RANGE_SEQ_IF *seq, void *seq_init_param,
                            uint n_ranges, uint mode,
                            HANDLER_BUFFER *buf);
  int multi_range_read_next(char **range_info);
private :
  /** The multi range read session object */
  DsMrr_impl ds_mrr;

  /*每个handler都有自己的insert buffer*/
  CflInsertBuffer *insert_buffer_;
  CflTable *cfl_table_;
  cfl_cursor_t cursor_;
  cfl_isearch_t isearch_;
  /*
    插入行到handler的插入缓冲区中，插入时不使用锁，在触发合并条件后，插入数据加入到cfl table的插入缓冲区中
  */
  int insert2buffer(cfl_dti_t key, void *row, uint16_t row_size);
  /*
    将缓冲区数据写入表中。
  */
  uint32_t test_count;
  void flush_buffer();
  /*
    获取下一条记录
    参数:
      over:输出参数，是否获取结束。true,获取记录成功;false,数据获取
    返回值:
      返回0，正确执行；小于0，执行错误。
  */
  int fetch_next(bool &over);
  /*
    定位到下一条记录。
  */
  int next(bool &over);
  /*
    取得页面内行的数据。
  */
  int fetch();
  /*
    根据isearch_中的信息，进行cursor定位，在cursor_中确定记录的位置
  */
  int locate_cursor();
  /*
    通过索引定位后的next
  */
  int locate_next(bool &over);
  int locate_next2(bool &over);
  /*
    获取物理行。所谓的物理行是指在page中存在的行，不论是删除的还是其他状态。
    参数：
      cursor,记录获取记录的位置信息
      over  ,是否在获取下一条记录时结束
  */
  int next_physical_row(CflTable *table, cfl_cursor_t &cursor, bool &over);
  /*
   与next_physical_row功能相同，只是从cursor所指位置向前移动
  */
  int prev_physical_row(cfl_cursor_t &cursor, bool &over)
  {
    return HA_ERR_WRONG_COMMAND;
  }

  /*
    索引定位时用于用于比较
  */
  bool locate_key_match();
  /*
    检查是否需要移动当前记录到下一条
  */
  bool check_renext();
  /*
    删除匹配的记录
  */
  int delete_rows(Field ** fields, cfl_dti_t key
                  , const uint8_t *row, uint32_t row_size);
  /*清除查询结束后cursor上的资源*/
  void clear_cursor();
  /*
    进行table的统计信息构造
  */
  void statistic();
};


#endif //_HA_CFL_H_
