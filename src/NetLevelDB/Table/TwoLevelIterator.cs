using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;

namespace NetLevelDB.Table
{
  internal delegate Iterator BlockFunctionDelegate(object arg, ReadOptions options, Slice index_value);

  internal class TwoLevelIterator : Iterator
  {
    private BlockFunctionDelegate block_function_;
    private object arg_;
    private ReadOptions options_;
    private Status status_;
    private IteratorWrapper index_iter_;
    private IteratorWrapper data_iter_; // May be NULL
    // If data_iter_ is non-NULL, then "data_block_handle_" holds the
    // "index_value" passed to block_function_ to create the data_iter_.
    private string data_block_handle_;

    public TwoLevelIterator(
      Iterator index_iter,
      BlockFunctionDelegate block_function,
      object arg,
      ReadOptions options)
    {
      block_function_ = block_function;
      arg_ = arg;
      options_ = options;
      index_iter_ = new IteratorWrapper(index_iter);
      data_iter_ = null;
    }    

    public override bool Valid
    {
      get { return data_iter_.Valid; }
    }

    public override void SeekToFirst()
    {
      index_iter_.SeekToFirst();
      InitDataBlock();
      
      if (data_iter_.Iterator != null) 
        data_iter_.SeekToFirst();
      
      SkipEmptyDataBlocksForward();
    }

    public override void SeekToLast()
    {
      index_iter_.SeekToLast();
  
      InitDataBlock();

      if (data_iter_.Iterator != null)
        data_iter_.SeekToLast();

      SkipEmptyDataBlocksBackward();
    }

    public override void Seek(Slice target)
    {
      index_iter_.Seek(target);

      InitDataBlock();

      if (data_iter_.Iterator != null) 
        data_iter_.Seek(target);
      
      SkipEmptyDataBlocksForward();
    }

    public override void Next()
    {
      Debug.Assert(Valid);
      data_iter_.Next();
      SkipEmptyDataBlocksForward();
    }

    public override void Prev()
    {
      Debug.Assert(Valid);
      data_iter_.Prev();
      SkipEmptyDataBlocksBackward();
    }

    public override Slice Key
    {
      get
      {
        Debug.Assert(Valid);
        return data_iter_.Key;
      }
    }

    public override Slice Value
    {
      get
      {
        Debug.Assert(Valid);
        return data_iter_.Key;
      }
    }

    public override Status Status
    {
      get
      {
        // It'd be nice if status() returned a const Status& instead of a Status
        if (!index_iter_.Status.IsOk)
        {
          return index_iter_.Status;
        }
        else if (data_iter_.Iterator != null && !data_iter_.Status.IsOk)
        {
          return data_iter_.Status;
        }
        else
        {
          return status_;
        }
      }
    }

    void SaveError(Status s) 
    {
    if (status_.IsOk && !s.IsOk) 
      status_ = s;
  }

  void SkipEmptyDataBlocksForward()
  {
    while (data_iter_.Iterator == null || !data_iter_.Valid)
    {
      // Move to next block
      if (!index_iter_.Valid)
      {
        SetDataIterator(null);
        return;
      }

      index_iter_.Next();
      InitDataBlock();
      
      if (data_iter_.Iterator != null) 
        data_iter_.SeekToFirst();
    }
  }

  void SkipEmptyDataBlocksBackward()
  {
    while (data_iter_.Iterator == null || !data_iter_.Valid)
    {
      // Move to next block
      if (!index_iter_.Valid)
      {
        SetDataIterator(null);
        return;
      }

      index_iter_.Prev();
      InitDataBlock();
      if (data_iter_.Iterator != null) 
        data_iter_.SeekToLast();
    }
  }
  
    void SetDataIterator(Iterator data_iter)
    {
      if (data_iter_.Iterator != null) 
        SaveError(data_iter_.Status);

      data_iter_.Iterator = data_iter;
    }
  
    void InitDataBlock()
    {
      if (!index_iter_.Valid)
      {
        SetDataIterator(null);
      }
      else
      {
        Slice handle = index_iter_.Value;
        if (data_iter_.Iterator != null && handle.Compare(data_block_handle_) == 0)
        {
          // data_iter_ is already constructed with this iterator, so
          // no need to change anything
        }
        else
        {
          Iterator iter = block_function_(arg_, options_, handle);
          
          data_block_handle_=  handle.Data.GetString(handle.Size);
          SetDataIterator(iter);
        }
      }
 
    }

  }
}