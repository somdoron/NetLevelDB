using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using NetLevelDB.CSharp;
using NetLevelDB.Util;

namespace NetLevelDB.Table.Format
{
	class Footer
	{
		public Footer()
		{

		}

		// The block handle for the metaindex block of the table
		public BlockHandle MetaindexHandle { get; set; }

		// The block handle for the index block of the table
		public BlockHandle IndexHandle { get; set; }

		public void EncodeTo(ref string dst)
		{
			int originalSize = dst.Length;

			MetaindexHandle.EncodeTo(ref dst);

			IndexHandle.EncodeTo(ref dst);

			dst = dst.Resize(2*BlockHandle.kMaxEncodedLength);
			
			Coding.PutFixed32(ref dst, (uint)(FormatHelper.kTableMagicNumber & 0xffffffffu));
			Coding.PutFixed32(ref dst, (uint)(FormatHelper.kTableMagicNumber >> 32));

			Debug.Assert(dst.Length == originalSize + kEncodedLength);
		}

		public Status DecodeFrom(ref Slice input)
		{
			ByteArrayPointer magicPtr = input.Data + (kEncodedLength - 8);
			
			UInt32 magicLo = Coding.DecodeFixed32(magicPtr);
			UInt32 magicHi = Coding.DecodeFixed32(magicPtr + 4);
			
			UInt64 magic = (((UInt64)(magicHi) << 32) | ((UInt64)(magicLo)));
			
			if (magic != FormatHelper.kTableMagicNumber)
			{
				return Status.InvalidArgument("not an sstable (bad magic number)");
			}

			Status result = MetaindexHandle.DecodeFrom(ref input);
			if (result.IsOk)
			{
				result = IndexHandle.DecodeFrom(ref input);
			}
			if (result.IsOk)
			{
				// We skip over any leftover data (just padding for now) in "input"
				ByteArrayPointer end = magicPtr + 8;
				input = new Slice(end, input.Data + input.Size - end);
			}
			
			return result;
		}

		// Encoded length of a Footer.  Note that the serialization of a
		// Footer will always occupy exactly this many bytes.  It consists
		// of two block handles and a magic number.
		public const int kEncodedLength = 2 * BlockHandle.kMaxEncodedLength + 8;
	}
}
