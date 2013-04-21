using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using NetLevelDB.Util;

namespace NetLevelDB.Table
{
  class MergingIterator : Iterator
  {
    // We might want to use a heap in case there are lots of children.
    // For now we use a simple array since we expect a very small number
    // of children in leveldb.
    Comparator comparator_;
    IteratorWrapper[] children_;    
    IteratorWrapper current_;

    // Which direction is the iterator moving?
    enum Direction
    {
      kForward,
      kReverse
    };
    Direction direction_;

    public MergingIterator(Comparator comparator, Iterator[] children)
    {
      comparator_ = comparator;
      children_ = new IteratorWrapper[children.Length];
      current_ = null;
      direction_ = Direction.kForward;
      for (int i = 0; i < children.Length; i++)
        children_[i] = new IteratorWrapper(children[i]);
    }

    public override bool Valid
    {
      get { return current_ != null; }
    }

    public override void SeekToFirst()
    {
      for (int i = 0; i < children_.Length; i++)
      {
        children_[i].SeekToFirst();
      }

      FindSmallest();
      direction_ = Direction.kForward;
    }

    public override void SeekToLast()
    {
      for (int i = 0; i < children_.Length; i++)
      {
        children_[i].SeekToLast();
      }

      FindLargest();
      direction_ = Direction.kReverse;
    }

    public override void Seek(Slice target)
    {
      for (int i = 0; i < children_.Length; i++)
      {
        children_[i].Seek(target);
      }

      FindSmallest();

      direction_ = Direction.kForward;
    }

    public override void Next()
    {
      Debug.Assert(Valid);

      // Ensure that all children are positioned after key().
      // If we are moving in the forward direction, it is already
      // true for all of the non-current_ children since current_ is
      // the smallest child and key() == current_->key().  Otherwise,
      // we explicitly position the non-current_ children.
      if (direction_ != Direction.kForward)
      {
        for (int i = 0; i < children_.Length; i++)
        {
          IteratorWrapper child = children_[i];
          if (child != current_)
          {
            child.Seek(Key);
            if (child.Valid &&
                comparator_.Compare(Key, child.Key) == 0)
            {
              child.Next();
            }
          }
        }
        direction_ = Direction.kForward;
      }

      current_.Next();
      FindSmallest();
    }

    public override void Prev()
    {
      Debug.Assert(Valid);

      // Ensure that all children are positioned before key().
      // If we are moving in the reverse direction, it is already
      // true for all of the non-current_ children since current_ is
      // the largest child and key() == current_->key().  Otherwise,
      // we explicitly position the non-current_ children.
      if (direction_ != Direction.kReverse)
      {
        for (int i = 0; i < children_.Length; i++)
        {
          IteratorWrapper child = children_[i];
          if (child != current_)
          {
            child.Seek(Key);
            if (child.Valid)
            {
              // Child is at first entry >= key().  Step back one to be < key()
              child.Prev();
            }
            else
            {
              // Child has no entries >= key().  Position at last entry.
              child.SeekToLast();
            }
          }
        }
        direction_ = Direction.kReverse;
      }

      current_.Prev();
      FindLargest();
    }

    public override Slice Key
    {
      get
      {
        Debug.Assert(Valid);

        return current_.Key;
      }
    }

    public override Slice Value
    {
      get
      {
        Debug.Assert(Valid);

        return current_.Value;
      }
    }

    public override Status Status
    {
      get
      {
        Status status = null;

        for (int i = 0; i < children_.Length; i++)
        {
          status = children_[i].Status;
          if (!status.IsOk)
          {
            break;
          }
        }

        return status;
      }
    }

    void FindSmallest()
    {
      IteratorWrapper smallest = null;
      for (int i = 0; i < children_.Length; i++)
      {
        IteratorWrapper child = children_[i];
        if (child.Valid)
        {
          if (smallest == null)
          {
            smallest = child;
          }
          else if (comparator_.Compare(child.Key, smallest.Key) < 0)
          {
            smallest = child;
          }
        }
      }
      current_ = smallest;
    }


    void FindLargest()
    {
      IteratorWrapper largest = null;
      for (int i = children_.Length - 1; i >= 0; i--)
      {
        IteratorWrapper child = children_[i];
        if (child.Valid)
        {
          if (largest == null)
          {
            largest = child;
          }
          else if (comparator_.Compare(child.Key, largest.Key) > 0)
          {
            largest = child;
          }
        }
      }
      current_ = largest;
    }

  }
}
