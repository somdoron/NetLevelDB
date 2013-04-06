using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using NetLevelDB.Util;

namespace NetLevelDB.DB
{
	/// <summary>
	/// MemTables are reference counted.  The initial reference count
	/// is zero and the caller must call Ref() at least once. 
	/// </summary>
	class MemTable
	{
		private int m_refs;
		private KeyComparator m_comparator;
		private SkipList m_table;
		
		public MemTable(InternalKeyComparator cmp)
		{
			m_comparator = new KeyComparator(cmp);
			m_refs = 0;
			m_table = new SkipList(m_comparator);
		}


		/// <summary>
		/// Increase reference count.
		/// </summary>
		public void Ref()
		{
			m_refs++;
		}

		/// <summary>
		/// Drop reference count.  Delete if no more references exist.
		/// </summary>
		public void UnRef()
		{
			--m_refs;
			Debug.Assert(m_refs >= 0);

			if (m_refs <= 0)
			{
				Dispose();				
			}
		}

		private void Dispose()
		{
			m_table = null;
			
			Debug.Assert(m_refs == 0);
		}		

		// Return an iterator that yields the contents of the memtable.
		//
		// The caller must ensure that the underlying MemTable remains live
		// while the returned iterator is live.  The keys returned by this
		// iterator are internal keys encoded by AppendInternalKey in the
		// db/format.{h,cc} module.
		public Iterator NewIterator()
		{
			return new MemTableIterator(m_table);
		}

		// Add an entry into memtable that maps key to value at the
		// specified sequence number and with the specified type.
		// Typically value will be empty if type==kTypeDeletion.
		public void Add(UInt64 seq, int type,
						 Slice key,
						 Slice value)
		{
			// Format of an entry is concatenation of:
			//  key_size     : varint32 of internal_key.size()
			//  key bytes    : char[internal_key.size()]
			//  value_size   : varint32 of value.size()
			//  value bytes  : char[value.size()]
			int key_size = key.Size;
			int val_size = value.Size;
			int internal_key_size = key_size + 8;
			int encoded_len =
					Coding.VarintLength(internal_key_size) + internal_key_size +
					Coding.VarintLength(val_size) + val_size;
			ByteArrayPointer buf = new ByteArrayPointer(encoded_len);
			ByteArrayPointer p = Coding.EncodeVarint32(buf, (uint)internal_key_size);

			key.Data.CopyTo(p, key_size);

			p += key_size;
			Coding.EncodeFixed64(p, (seq << 8) | (UInt64)type);

			p += 8;
			p = Coding.EncodeVarint32(p, (uint)val_size);

			value.Data.CopyTo(p, val_size);

			Debug.Assert((p + val_size) - buf == encoded_len);
			m_table.Insert(buf);
		}

		// If memtable contains a value for key, store it in *value and return true.
		// If memtable contains a deletion for key, store a NotFound() error
		// in *status and return true.
		// Else, return false.
		public bool Get(LookupKey key, ref ByteArrayPointer value, ref Status s)
		{
			Slice memkey = key.GetMemTableKey();
			SkipList.Iterator iter = new SkipList.Iterator(m_table);
			iter.Seek(memkey.Data);
			if (iter.Valid)
			{
				// entry format is:
				//    klength  varint32
				//    userkey  char[klength]
				//    tag      uint64
				//    vlength  varint32
				//    value    char[vlength]
				// Check that it belongs to same user key.  We do not check the
				// sequence number since the Seek() call above should have skipped
				// all entries with overly large sequence numbers.
				ByteArrayPointer entry = iter.Key;
				UInt32 key_length;
				ByteArrayPointer key_ptr = Coding.GetVarint32Ptr(entry, entry + 5, out key_length);
				if (m_comparator.Comparator.UserComparator.Compare(
								new Slice(key_ptr, (int)key_length - 8),
								key.GetUserKey()) == 0)
				{
					// Correct user key
					UInt64 tag = Coding.DecodeFixed64(key_ptr+ ((int)key_length - 8));
					switch ((ValueTypeEnum)(tag & 0xff))
					{
						case ValueTypeEnum.kTypeValue:
							{
								Slice v = Coding.GetLengthPrefixedSlice(key_ptr+((int)key_length));
								value = new ByteArrayPointer(v.Data, v.Size);
								return true;
							}
						case ValueTypeEnum.kTypeDeletion:
							s = Status.NotFound(new Slice());
							return true;
					}
				}
			}
			return false;
		}
	}
}
