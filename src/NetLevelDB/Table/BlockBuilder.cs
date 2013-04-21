using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using NetLevelDB.CSharp;
using NetLevelDB.Util;

namespace NetLevelDB.Table
{
	/// <summary>
	///BlockBuilder generates blocks where keys are prefix-compressed:
	///
	/// When we store a key, we drop the prefix shared with the previous
	/// string.  This helps reduce the space requirement significantly.
	/// Furthermore, once every K keys, we do not apply the prefix
	/// compression and store the entire key.  We call this a "restart
	/// point".  The tail end of the block stores the offsets of all of the
	/// restart points, and can be used to do a binary search when looking
	/// for a particular key.  Values are stored as-is (without compression)
	/// immediately following the corresponding key.
	///
	/// An entry for a particular key-value pair has the form:
	///     shared_bytes: varint32
	///     unshared_bytes: varint32
	///     value_length: varint32
	///     key_delta: char[unshared_bytes]
	///     value: char[value_length]
	/// shared_bytes == 0 for restart points.
	///
	/// The trailer of the block has the form:
	///     restarts: uint32[num_restarts]
	///     num_restarts: uint32
	/// restarts[i] contains the offset within the block of the ith restart point.

	/// </summary>
	class BlockBuilder
	{
		Options options_;
		string buffer_;      // Destination buffer
		IList<UInt32> restarts_;    // Restart points
		int counter_;     // Number of entries emitted since restart
		bool finished_;    // Has Finish() been called?
		string last_key_;

		public BlockBuilder(Options options)
		{
			options_ = options;
			restarts_ = new List<uint>();
			counter_ = 0;
			finished_ = false;

			Debug.Assert(options.BlockRestartInterval >= 1);
			restarts_.Add(0);       // First restart point is at offset 0
		}

		// Reset the contents as if the BlockBuilder was just constructed.
		public void Reset()
		{
			buffer_ = string.Empty;
			restarts_.Clear();
			restarts_.Add(0);       // First restart point is at offset 0
			counter_ = 0;
			finished_ = false;
			last_key_ = string.Empty;
		}

		// REQUIRES: Finish() has not been callled since the last call to Reset().
		// REQUIRES: key is larger than any previously added key
		public void Add(Slice key, Slice value)
		{
			Slice last_key_piece = new Slice(last_key_);
			Debug.Assert(!finished_);
			Debug.Assert(counter_ <= options_.BlockRestartInterval);
			Debug.Assert(string.IsNullOrEmpty(buffer_) // No values yet?
						 || options_.Comparator.Compare(key, last_key_piece) > 0);
			int shared = 0;
			if (counter_ < options_.BlockRestartInterval)
			{
				// See how much sharing to do with previous string
				int min_length = Math.Min(last_key_piece.Size, key.Size);
				while ((shared < min_length) && (last_key_piece[shared] == key[shared]))
				{
					shared++;
				}
			}
			else
			{
				// Restart compression
				restarts_.Add((uint)buffer_.Length);
				counter_ = 0;
			}
			int non_shared = key.Size - shared;

			// Add "<shared><non_shared><value_size>" to buffer_
			Coding.PutVarint32(ref buffer_, (uint)shared);
			Coding.PutVarint32(ref buffer_, (uint)non_shared);
			Coding.PutVarint32(ref buffer_, (uint)value.Size);

			// Add string delta to buffer_ followed by value
			buffer_ += (key.Data + shared).GetString(non_shared);
			buffer_ += value.Data.GetString(value.Size);

			// Update state
			last_key_ = last_key_.Resize(shared);
			last_key_ += (key.Data + shared).GetString(non_shared);
			Debug.Assert(new Slice(last_key_) == key);
			counter_++;
		}

		// Finish building the block and return a slice that refers to the
		// block contents.  The returned slice will remain valid for the
		// lifetime of this builder or until Reset() is called.
		public Slice Finish()
		{
			// Append restart array
			for (int i = 0; i < restarts_.Count; i++)
			{
				Coding.PutFixed32(ref buffer_, restarts_[i]);
			}
			Coding.PutFixed32(ref buffer_, (uint)restarts_.Count);
			finished_ = true;
			return new Slice(buffer_);
		}

		// Returns an estimate of the current (uncompressed) size of the block
		// we are building.
		public int CurrentSizeEstimate
		{
			get
			{
				return (buffer_.Length +                        // Raw data buffer
				restarts_.Count * sizeof(UInt32) +   // Restart array
				sizeof(UInt32));                      // Restart array length

			}
		}

		// Return true iff no entries have been added since the last Reset()
		public bool Empty
		{
			get { return string.IsNullOrEmpty(buffer_); }

		}

	}
}
