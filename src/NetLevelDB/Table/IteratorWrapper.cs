using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;

namespace NetLevelDB.Table
{
  internal class IteratorWrapper
  {
    private Iterator iter_;
    private bool valid_;
    private Slice key_;

    public IteratorWrapper()
    {
      iter_ = null;
      valid_ = false;
    }

    public IteratorWrapper(Iterator iter)
    {
      Iterator = iter;
    }

    public Iterator Iterator
    {
      get { return iter_; }
      set
      {
        iter_ = value;
        if (iter_ == null)
        {
          valid_ = false;
        }
        else
        {
          Update();

        }

      }
    }

    private void Update()
    {
      valid_ = iter_.Valid;
      if (valid_)
      {
        key_ = iter_.Key;
      }
    }


    // Iterator interface methods
    public bool Valid
    {
      get { return valid_; }
    }

    public Slice Key
    {
      get
      {
        Debug.Assert(Valid);
        return key_;
      }
    }

    public Slice Value
    {
      get
      {
        Debug.Assert(Valid);
        return iter_.Value;
      }
    }

    // Methods below require iter() != NULL
    public Status Status
    {
      get
      {
        Debug.Assert(iter_ != null);
        return iter_.Status;
      }
    }


    public void Next()
    {
      Debug.Assert(iter_ != null);
      iter_.Next();
      Update();
    }

    public void Prev()
    {
      Debug.Assert(iter_ != null); 
      iter_.Prev(); 
      Update();
    }

    public void Seek(Slice k)
    {
      Debug.Assert(iter_ != null); 
      iter_.Seek(k); 
      Update();
    }
    
    public void SeekToFirst()
    {
      Debug.Assert(iter_ != null);  
      iter_.SeekToFirst(); 
      Update();
    }
    
    public void SeekToLast()
    {
      Debug.Assert(iter_ != null); 
      iter_.SeekToLast(); 
      Update();
    }
  }
}
