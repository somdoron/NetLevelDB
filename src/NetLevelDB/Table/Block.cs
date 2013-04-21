using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using NetLevelDB.CSharp;
using NetLevelDB.Table.Format;
using NetLevelDB.Util;

namespace NetLevelDB.Table
{
	class Block
	{
		ByteArrayPointer data_;
		int size_;
		UInt32 restart_offset_;     // Offset in data_ of restart array
		bool owned_;                  // Block owns data_[]  

		// Initialize the block with the specified contents.
		public Block(BlockContents contents)
		{
			data_ = contents.Data.Data;
			size_ = contents.Data.Size;
			owned_ = contents.HeapAllocated;

			if (size_ < sizeof(UInt32))
			{
				size_ = 0;  // Error marker
			}
			else
			{
				restart_offset_ = (uint)(size_ - (1 + NumRestarts()) * sizeof(UInt32));
				if (restart_offset_ > size_ - sizeof(UInt32))
				{
					// The size is too small for NumRestarts() and therefore
					// restart_offset_ wrapped around.
					size_ = 0;
				}
			}
		}

		public int Size { get { return size_; } }

		private UInt32 NumRestarts()
		{
			Debug.Assert(size_ >= 2 * sizeof(UInt32));

			return Coding.DecodeFixed32(data_ + (size_ - sizeof(UInt32)));
		}

		// Helper routine: decode the next block entry starting at "p",
		// storing the number of shared key bytes, non_shared key bytes,
		// and the length of the value in "*shared", "*non_shared", and
		// "*value_length", respectively.  Will not derefence past "limit".
		//
		// If any errors are detected, returns NULL.  Otherwise, returns a
		// pointer to the key delta (just past the three decoded values).
		private static ByteArrayPointer DecodeEntry(ByteArrayPointer p, ByteArrayPointer limit,
																				 out UInt32 shared,
																				 out UInt32 non_shared,
																				 out UInt32 value_length)
		{
			if (limit - p < 3)
			{
				shared = non_shared = value_length = 0;
				return ByteArrayPointer.Null;
			}
			shared = p[0];
			non_shared = p[1];
			value_length = p[3];
			if ((shared | non_shared | value_length) < 128)
			{
				// Fast path: all three values are encoded in one byte each
				p += 3;
			}
			else
			{
				if ((p = Coding.GetVarint32Ptr(p, limit, out shared)) == ByteArrayPointer.Null) return ByteArrayPointer.Null;
				if ((p = Coding.GetVarint32Ptr(p, limit, out non_shared)) == ByteArrayPointer.Null) return ByteArrayPointer.Null;
				if ((p = Coding.GetVarint32Ptr(p, limit, out value_length)) == ByteArrayPointer.Null) return ByteArrayPointer.Null;
			}

			if ((limit - p) < (non_shared + value_length))
			{
				return ByteArrayPointer.Null;
			}
			return p;
		}

		public Iterator NewIterator(Comparator comparator)
		{
			if (size_ < 2 * sizeof(UInt32))
			{
				return Iterator.NewErrorIterator(Status.Corruption("bad block contents"));
			}
			UInt32 num_restarts = NumRestarts();
			if (num_restarts == 0)
			{
				return Iterator.EmptyIterator;
			}
			else
			{
				return new Iter(comparator, data_, restart_offset_, num_restarts);
			}
		}

		class Iter : Iterator
		{

			private Comparator comparator_;
			private ByteArrayPointer data_;      // underlying block contents
			UInt32 restarts_;     // Offset of restart array (list of fixed32)
			UInt32 num_restarts_; // Number of uint32_t entries in restart array

			// current_ is offset in data_ of current entry.  >= restarts_ if !Valid
			UInt32 current_;
			UInt32 restart_index_;  // Index of restart block in which current_ falls

			string key_;
			Slice value_;
			Status status_;


			int Compare(Slice a, Slice b)
			{
				return comparator_.Compare(a, b);
			}

			// Return the offset in data_ just past the end of the current entry.
			UInt32 NextEntryOffset()
			{
				return (uint)((value_.Data + value_.Size) - data_);
			}

			UInt32 GetRestartPoint(UInt32 index)
			{
				Debug.Assert(index < num_restarts_);
				return Coding.DecodeFixed32(data_ + (int)(restarts_ + index * sizeof(UInt32)));
			}

			void SeekToRestartPoint(UInt32 index)
			{
				key_ = string.Empty;
				restart_index_ = index;
				// current_ will be fixed by ParseNextKey();

				// ParseNextKey() starts at the end of value_, so set value_ accordingly
				UInt32 offset = GetRestartPoint(index);
				value_ = new Slice(data_ + (int)offset, 0);
			}

			public Iter(Comparator comparator,
					 ByteArrayPointer data,
					 UInt32 restarts,
					 UInt32 num_restarts)
			{
				comparator_ = comparator;
				data_ = data;
				restarts_ = restarts;
				num_restarts_ = num_restarts;
				current_ = restarts_;
				restart_index_ = num_restarts_;
				Debug.Assert(num_restarts_ > 0);
			}


			public override bool Valid
			{
				get { return current_ < restarts_; }
			}

			public override Status Status
			{
				get { return status_; }
			}

			public override Slice Key
			{
				get
				{
					Debug.Assert(Valid);
					return new Slice(key_);
				}
			}

			public override Slice Value
			{
				get
				{
					Debug.Assert(Valid);
					return value_;
				}
			}

			public override void Next()
			{
				Debug.Assert(Valid);
				ParseNextKey();
			}

			public override void Prev()
			{
				Debug.Assert(Valid);


				// Scan backwards to a restart point before current_
				UInt32 original = current_;
				while (GetRestartPoint(restart_index_) >= original)
				{
					if (restart_index_ == 0)
					{
						// No more entries
						current_ = restarts_;
						restart_index_ = num_restarts_;
						return;
					}
					restart_index_--;
				}

				SeekToRestartPoint(restart_index_);
				do
				{
					// Loop until end of current entry hits the start of original entry
				} while (ParseNextKey() && NextEntryOffset() < original);
			}

			public override void Seek(Slice target)
			{
				// Binary search in restart array to find the last restart point
				// with a key < target
				UInt32 left = 0;
				UInt32 right = num_restarts_ - 1;

				while (left < right)
				{
					UInt32 mid = (left + right + 1) / 2;
					UInt32 region_offset = GetRestartPoint(mid);
					UInt32 shared, non_shared, value_length;
					ByteArrayPointer key_ptr = Block.DecodeEntry(data_ + (int)region_offset, data_ + (int)restarts_,
																						out shared, out non_shared, out value_length);
					if (key_ptr.IsNull || (shared != 0))
					{
						CorruptionError();
						return;
					}
					Slice mid_key = new Slice(key_ptr, (int)non_shared);
					if (Compare(mid_key, target) < 0)
					{
						// Key at "mid" is smaller than "target".  Therefore all
						// blocks before "mid" are uninteresting.
						left = mid;
					}
					else
					{
						// Key at "mid" is >= "target".  Therefore all blocks at or
						// after "mid" are uninteresting.
						right = mid - 1;
					}
				}

				// Linear search (within restart block) for first key >= target
				SeekToRestartPoint(left);
				while (true)
				{
					if (!ParseNextKey())
					{
						return;
					}
					if (Compare(new Slice(key_), target) >= 0)
					{
						return;
					}
				}
			}

			public override void SeekToFirst()
			{
				SeekToRestartPoint(0);
				ParseNextKey();
			}

			public override void SeekToLast()
			{
				SeekToRestartPoint(num_restarts_ - 1);
				while (ParseNextKey() && NextEntryOffset() < restarts_)
				{
					// Keep skipping
				}
			}

			private void CorruptionError()
			{
				current_ = restarts_;
				restart_index_ = num_restarts_;
				status_ = Status.Corruption("bad entry in block");
				key_ = string.Empty;
				value_.Clear();
			}

			bool ParseNextKey()
			{
				current_ = NextEntryOffset();
				ByteArrayPointer p = data_ + (int)current_;
				ByteArrayPointer limit = data_ + (int)restarts_;  // Restarts come right after data
				if (p >= limit)
				{
					// No more entries to return.  Mark as invalid.
					current_ = restarts_;
					restart_index_ = num_restarts_;
					return false;
				}

				// Decode next entry
				UInt32 shared, non_shared, value_length;
				p = Block.DecodeEntry(p, limit, out shared, out non_shared, out value_length);
				if (p.IsNull || key_.Length < shared)
				{
					CorruptionError();
					return false;
				}
				else
				{
					key_ = key_.Resize((int)shared);
					key_ += p.GetString((int)non_shared);
					
					value_ = new Slice(p + (int)non_shared, (int)value_length);
					while (restart_index_ + 1 < num_restarts_ &&
								 GetRestartPoint(restart_index_ + 1) < current_)
					{
						++restart_index_;
					}
					return true;
				}
			}
		}
	}
}
