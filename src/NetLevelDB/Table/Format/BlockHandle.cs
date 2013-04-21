using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using NetLevelDB.Util;

namespace NetLevelDB.Table.Format
{
	public class BlockHandle
	{
		// Maximum encoding length of a BlockHandle
		public const int kMaxEncodedLength = 10 + 10;

		public BlockHandle()
		{
			Offset = 0;
			Size = 0;
		}

		public UInt64 Offset { get; set; }
		public UInt64 Size { get; set; }

		public void EncodeTo(ref string dst)
		{
			// Sanity check that all fields have been set
			Debug.Assert(Offset != 0);
			Debug.Assert(Size != 0);

			Coding.PutVarint64(ref dst, Offset);
			Coding.PutVarint64(ref dst, Size);
		}

		public Status DecodeFrom(ref Slice input)
		{
			UInt64 offset;
			UInt64 size;

			if (Coding.GetVarint64(ref input, out offset) &&
					Coding.GetVarint64(ref input, out size))
			{
				Offset = offset;
				Size = size;

				return Status.OK;
			}
			else
			{
				return Status.Corruption("bad block handle");
			}
		}		
	}
}
