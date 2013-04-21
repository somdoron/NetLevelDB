using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using NetLevelDB.CSharp;
using NetLevelDB.Port;
using NetLevelDB.Util;

namespace NetLevelDB.DB
{

	/// <summary>
	///
	/// Thread safety
	/// -------------
	///
	/// Writes require external synchronization, most likely a mutex.
	/// Reads require a guarantee that the SkipList will not be destroyed
	/// while the read is in progress.  Apart from that, reads progress
	/// without any internal locking or synchronization.
	///
	/// Invariants:
	///
	/// (1) Allocated nodes are never deleted until the SkipList is
	/// destroyed.  This is trivially guaranteed by the code since we
	/// never delete any skip list nodes.
	///
	/// (2) The contents of a Node except for the next/prev pointers are
	/// immutable after the Node has been linked into the SkipList.
	/// Only Insert() modifies the list, and it is careful to initialize
	/// a node and use release-stores to publish the nodes in one or
	/// more lists.
	///
	/// ... prev vs. next pointer ordering ...

	/// </summary>
	class SkipList
	{
		private const int kMaxHeight = 12;

		// Immutable after construction
		private KeyComparator compare_;
		
		private Node head_;

		// Modified only by Insert().  Read racily by readers, but stale
		// values are ok.
		private AtomicInteger max_height_; // Height of the entire list

		// Read/written only by Insert().
		private Random rnd_;


		// Create a new SkipList object that will use "cmp" for comparing keys,
		// and will allocate memory using "*arena".  Objects allocated in the arena
		// must remain allocated for the lifetime of the skiplist object.
		public SkipList(KeyComparator cmp)
		{
			compare_ = cmp;
			
			head_ = NewNode(new ByteArrayPointer(0), kMaxHeight);
			max_height_ = new AtomicInteger(1);
			// 0xdeadbeef
			rnd_ = new Random(-559038737);
			for (int i = 0; i < kMaxHeight; i++)
			{
				head_.SetNext(i, null);
			}
		}

		// Insert key into the list.
		// REQUIRES: nothing that compares equal to key is currently in the list.
		public void Insert(ByteArrayPointer key)
		{
			// TODO(opt): We can use a barrier-free variant of FindGreaterOrEqual()
			// here since Insert() is externally synchronized.
			Node[] prev = new Node[kMaxHeight];
			Node x = FindGreaterOrEqual(key, prev);

			// Our data structure does not allow duplicate insertion
			Debug.Assert(x == null || !Equal(key, x.Key));

			int height = RandomHeight();
			if (height > MaxHeight)
			{
				for (int i = MaxHeight; i < height; i++)
				{
					prev[i] = head_;
				}
				//fprintf(stderr, "Change height from %d to %d\n", max_height_, height);

				// It is ok to mutate max_height_ without any synchronization
				// with concurrent readers.  A concurrent reader that observes
				// the new value of max_height_ will see either the old value of
				// new level pointers from head_ (NULL), or a new value set in
				// the loop below.  In the former case the reader will
				// immediately drop to the next level since NULL sorts after all
				// keys.  In the latter case the reader will use the new node.
				max_height_.NoBarrierStore(height);
			}

			x = NewNode(key, height);

			for (int i = 0; i < height; i++)
			{
				// NoBarrier_SetNext() suffices since we will add a barrier when
				// we publish a pointer to "x" in prev[i].
				x.NoBarrier_SetNext(i, prev[i].NoBarrierNext(i));
				prev[i].SetNext(i, x);
			}
		}

		// Returns true iff an entry that compares equal to key is in the list.
		public bool Contains(ByteArrayPointer key)
		{
			Node x = FindGreaterOrEqual(key, null);
			if (x != null && Equal(key, x.Key))
			{
				return true;
			}
			else
			{
				return false;
			}
		}

		// Iteration over the contents of a skip list
		public class Iterator 
		{
			private SkipList list_;
			private Node node_;

			// Initialize an iterator over the specified list.
			// The returned iterator is not valid.
			public Iterator(SkipList list)
			{
				list_ = list;
				node_ = null;
			}

			// Returns true iff the iterator is positioned at a valid node.
			public bool Valid
			{
				get { return node_ != null; }
			}

			// Returns the key at the current position.
			// REQUIRES: Valid()
			public ByteArrayPointer Key
			{
				get
				{
					Debug.Assert(Valid);
					return node_.Key;
				}
			}

			// Advances to the next position.
			// REQUIRES: Valid()
			public void Next()
			{
				Debug.Assert(Valid);
				node_ = node_.Next(0);
			}

			// Advances to the previous position.
			// REQUIRES: Valid()
			public void Prev()
			{
				// Instead of using explicit "prev" links, we just search for the
				// last node that falls before key.
				Debug.Assert(Valid);
				node_ = list_.FindLessThan(node_.Key);
				if (node_ == list_.head_)
				{
					node_ = null;
				}
			}

			// Advance to the first entry with a key >= target
			public void Seek(ByteArrayPointer target)
			{
				node_ = list_.FindGreaterOrEqual(target, null);
			}

			// Position at the first entry in list.
			// Final state of iterator is Valid() iff list is not empty.
			public void SeekToFirst()
			{
				node_ = list_.head_.Next(0);				
			}

			// Position at the last entry in list.
			// Final state of iterator is Valid() iff list is not empty.
			public void SeekToLast()
			{
				node_ = list_.FindLast();
				if (node_ == list_.head_)
				{
					node_ = null;
				}
			}
		};

		private int MaxHeight
		{

			get { return max_height_.NoBarrierLoad(); }

		}

		private Node NewNode(ByteArrayPointer key, int height)
		{		
			var node  = new Node(key, height);
			
			return node;
		}

		private int RandomHeight()
		{
			// Increase height with probability 1 in kBranching
			const uint kBranching = 4;
			int height = 1;
			while (height < kMaxHeight && ((rnd_.Next() % kBranching) == 0))
			{
				height++;
			}
			Debug.Assert(height > 0);
			Debug.Assert(height <= kMaxHeight);
			return height;
		}


		private bool Equal(ByteArrayPointer a, ByteArrayPointer b)
		{
			return (compare_.Compare(a, b) == 0); 
		}

		// Return true if key is greater than the data stored in "n"
		private bool KeyIsAfterNode(ByteArrayPointer key, Node n)
		{
			// NULL n is considered infinite
			//return (n != null) && (compare_(n->key, key) < 0);
			return (n != null) && (compare_.Compare(n.Key, key) < 0);
		}

		// Return the earliest node that comes at or after key.
		// Return NULL if there is no such node.
		//
		// If prev is non-NULL, fills prev[level] with pointer to previous
		// node at "level" for every level in [0..max_height_-1].
		private Node FindGreaterOrEqual(ByteArrayPointer key, Node[] prev)
		{
			Node x = head_;
			int level = MaxHeight - 1;
			while (true)
			{
				Node next = x.Next(level);
				if (KeyIsAfterNode(key, next))
				{
					// Keep searching in this list
					x = next;
				}
				else
				{
					if (prev != null) prev[level] = x;
					if (level == 0)
					{
						return next;
					}
					else
					{
						// Switch to next list
						level--;
					}
				}
			}
		}

		// Return the latest node with a key < key.
		// Return head_ if there is no such node.
		private Node FindLessThan(ByteArrayPointer key)
		{
			Node x = head_;
			int level = MaxHeight - 1;
			while (true)
			{
				Debug.Assert(x == head_ || compare_.Compare(x.Key, key) < 0);
				Node next = x.Next(level);
				if (next == null || compare_.Compare(next.Key, key) >= 0)
				{
					if (level == 0)
					{
						return x;
					}
					else
					{
						// Switch to next list
						level--;
					}
				}
				else
				{
					x = next;
				}
			}
		}

		// Return the last node in the list.
		// Return head_ if list is empty.
		private Node FindLast()
		{
			Node x = head_;
			int level = MaxHeight - 1;
			while (true)
			{
				Node next = x.Next(level);
				if (next == null)
				{
					if (level == 0)
					{
						return x;
					}
					else
					{
						// Switch to next list
						level--;
					}
				}
				else
				{
					x = next;
				}
			}
		}

		// Implementation details follow
		public class Node
		{
			public Node(ByteArrayPointer k, int height)
			{
				Key = k;
				next_ = new AtomicPointer<Node>[height];
			}

			public ByteArrayPointer Key;

			private AtomicPointer<Node>[] next_;			

			// Accessors/mutators for links.  Wrapped in methods so we can
			// add the appropriate barriers as necessary.
			public Node Next(int n)
			{
				Debug.Assert(n >= 0);

				// Use an 'acquire load' so that we observe a fully initialized
				// version of the returned Node.
				return next_[n].AcquireLoad();
			}

			public void SetNext(int n, Node x)
			{
				Debug.Assert(n >= 0);
				// Use a 'release store' so that anybody who reads through this
				// pointer observes a fully initialized version of the inserted node.
				next_[n].ReleaseStore(x);
			}

			// No-barrier variants that can be safely used in a few locations.
			public Node NoBarrierNext(int n)
			{
				Debug.Assert(n >= 0);
				return next_[n].NoBarrierLoad();
			}

			public void NoBarrier_SetNext(int n, Node x)
			{
				Debug.Assert(n >= 0);
				next_[n].NoBarrierStore(x);
			}
		}		
	}
}


