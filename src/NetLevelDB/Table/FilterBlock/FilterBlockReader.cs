using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NetLevelDB.CSharp;
using NetLevelDB.Util;

namespace NetLevelDB.Table.FilterBlock
{
  class FilterBlockReader
  {
    FilterPolicy policy_;
    ByteArrayPointer data_;    // Pointer to filter data (at block-start)
    ByteArrayPointer offset_;  // Pointer to beginning of offset array (at block-end)
    int num_;          // Number of entries in offset array
    int base_lg_;      // Encoding parameter (see kFilterBaseLg in .cc file)


    // REQUIRES: "contents" and *policy must stay live while *this is live.
    public FilterBlockReader(FilterPolicy policy, Slice contents)
    {
      policy_ = policy;
      data_ = ByteArrayPointer.Null;
      offset_ = ByteArrayPointer.Null;
      num_ = 0;
      base_lg_ = 0;

      int n = contents.Size;
      if (n < 5) return;  // 1 byte for base_lg_ and 4 for start of offset array
      base_lg_ = contents[n - 1];
      UInt32 last_word = Coding.DecodeFixed32(contents.Data + n - 5);
      if (last_word > n - 5) return;
      data_ = contents.Data;
      offset_ = data_ + (int)last_word;
      num_ = (int)((n - 5 - last_word) / 4);
    }

    public bool KeyMayMatch(UInt64 block_offset, Slice key)
    {
      UInt64 index = block_offset >> base_lg_;
      if (index < (ulong)num_)
      {
        UInt32 start = Coding.DecodeFixed32(offset_ + (int)(index * 4));
        UInt32 limit = Coding.DecodeFixed32(offset_ + (int)(index * 4 + 4));
        if (start <= limit && limit <= (offset_ - data_))
        {
          Slice filter = new Slice(data_ + (int)start, (int)(limit - start));
          return policy_.KeyMayMatch(key, filter);
        }
        else if (start == limit)
        {
          // Empty filters do not match any keys
          return false;
        }
      }
      return true;  // Errors are treated as potential matches

    }
  }
}
