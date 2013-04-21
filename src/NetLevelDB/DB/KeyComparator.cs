using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NetLevelDB.CSharp;
using NetLevelDB.DB;
using NetLevelDB.Util;

namespace NetLevelDB.DB
{
	class KeyComparator
	{
		public KeyComparator(InternalKeyComparator c)
		{
			Comparator = c;
		} 

		public InternalKeyComparator Comparator { get; private set; }

		public int Compare(ByteArrayPointer aptr, ByteArrayPointer bptr)
		{
			// Internal keys are encoded as length-prefixed strings.
			Slice a = MemTable.GetLengthPrefixedSlice(aptr);
			Slice b = MemTable.GetLengthPrefixedSlice(bptr);
			return Comparator.Compare(a, b);
		}
	}
}
