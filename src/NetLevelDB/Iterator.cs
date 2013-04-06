using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Diagnostics;

namespace NetLevelDB
{
	public abstract class Iterator : IDisposable
	{
		class Cleanup
		{
			public CleanupFunction Function { get; set; }
			public object Arg1 { get; set; }
			public object Arg2 { get; set; }
			public Cleanup Next { get; set; }
		};

		Cleanup cleanup_;

		public Iterator()
		{
			cleanup_ = new Cleanup();
			cleanup_.Function = null;
			cleanup_.Next = null;
		}

		public void Dispose()
		{
			if (cleanup_.Function != null)
			{
				cleanup_.Function(cleanup_.Arg1, cleanup_.Arg2);
				
				for (Cleanup c = cleanup_.Next; c != null; )
				{
					c.Function(c.Arg1, c.Arg2);
					Cleanup next = c.Next;

					c.Function = null;
					c.Next = null;
					c.Arg1 = null;
					c.Arg2 = null;

					c = next;
				}

				cleanup_.Function = null;
				cleanup_.Next = null;
				cleanup_.Arg1 = null;
				cleanup_.Arg2 = null;
			}

			cleanup_.Function = null;
			cleanup_.Next = null;
		}

		public static Iterator EmptyIterator
		{
			get { return new EmptyIterator(Status.OK); }
		}

		public static Iterator NewErrorIterator(Status status)
		{
			return new EmptyIterator(status);
		}

		// An iterator is either positioned at a key/value pair, or
		// not valid.  This method returns true iff the iterator is valid.
		public abstract bool Valid { get; }

		// Position at the first key in the source.  The iterator is Valid()
		// after this call iff the source is not empty.
		public abstract void SeekToFirst();

		// Position at the last key in the source.  The iterator is
		// Valid() after this call iff the source is not empty.
		public abstract void SeekToLast();

		// Position at the first key in the source that at or past target
		// The iterator is Valid() after this call iff the source contains
		// an entry that comes at or past target.
		public abstract void Seek(Slice target);

		// Moves to the next entry in the source.  After this call, Valid() is
		// true iff the iterator was not positioned at the last entry in the source.
		// REQUIRES: Valid()
		public abstract void Next();

		// Moves to the previous entry in the source.  After this call, Valid() is
		// true iff the iterator was not positioned at the first entry in source.
		// REQUIRES: Valid()
		public abstract void Prev();

		// Return the key for the current entry.  The underlying storage for
		// the returned slice is valid only until the next modification of
		// the iterator.
		// REQUIRES: Valid()
		public abstract Slice Key { get; }

		// Return the value for the current entry.  The underlying storage for
		// the returned slice is valid only until the next modification of
		// the iterator.
		// REQUIRES: !AtEnd() && !AtStart()
		public abstract Slice Value { get; }

		// If an error has occurred, return it.  Else return an ok status.
		public abstract Status Status { get; }

		// Clients are allowed to register function/arg1/arg2 triples that
		// will be invoked when this iterator is destroyed.
		//
		// Note that unlike all of the preceding methods, this method is
		// not abstract and therefore clients should not override it.
		public delegate void CleanupFunction(object arg1, object arg2);

		public void RegisterCleanup(CleanupFunction function, object arg1, object arg2)
		{
			Debug.Assert(function != null);
			Cleanup c;

			if (cleanup_.Function == null)
			{
				c = cleanup_;
			}
			else
			{
				c = new Cleanup();
				c.Next = cleanup_.Next;
				cleanup_.Next = c;
			}
			c.Function = function;
			c.Arg1 = arg1;
			c.Arg2 = arg2;

		}



	}
}
