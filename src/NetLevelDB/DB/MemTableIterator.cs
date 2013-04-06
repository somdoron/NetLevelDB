﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NetLevelDB.Util;

namespace NetLevelDB.DB
{
	class MemTableIterator : Iterator
	{
		private SkipList.Iterator m_iterator;
		private ByteArrayPointer m_tmp;

		public MemTableIterator(SkipList table)
		{
			m_iterator = new SkipList.Iterator(table);
		}

		public override bool Valid
		{
			get { return m_iterator.Valid; }
		}

		public override void Seek(Slice target)
		{
			m_iterator.Seek(EncodeKey(out m_tmp, target));
		}

		public override void SeekToFirst()
		{
			m_iterator.SeekToFirst();
		}

		public override void SeekToLast()
		{
			m_iterator.SeekToLast();
		}

		// Encode a suitable internal key target for "target" and return it.
		// Uses *scratch as scratch space, and the returned pointer will point
		// into this scratch space.
		static ByteArrayPointer EncodeKey(out ByteArrayPointer scratch, Slice target)
		{
			int size = Coding.VarintLength(target.Size) + target.Size;

			scratch = new ByteArrayPointer(size);

			scratch = Coding.EncodeVarint32(scratch, (uint)target.Size);

			target.Data.CopyTo(scratch, target.Size);

			return new ByteArrayPointer(scratch.Data);
		}

		public override void Next()
		{
			m_iterator.Next();
		}

		public override void Prev()
		{
			m_iterator.Prev();			
		}

		public override Slice Key
		{
			get { return Coding.GetLengthPrefixedSlice(m_iterator.Key); }
		}

		public override Slice Value
		{
			get
			{
				Slice key_slice = Coding.GetLengthPrefixedSlice(m_iterator.Key);
				return Coding.GetLengthPrefixedSlice(key_slice.Data + key_slice.Size);
			}
		}

		public override Status Status
		{
			get { return Status.OK; }
		}
	}
}
