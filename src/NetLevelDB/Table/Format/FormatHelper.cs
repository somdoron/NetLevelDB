using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NetLevelDB.CSharp;
using NetLevelDB.Environment;
using NetLevelDB.Util;

namespace NetLevelDB.Table.Format
{
	static class FormatHelper
	{
		// kTableMagicNumber was picked by running
		//    echo http://code.google.com/p/leveldb/ | sha1sum
		// and taking the leading 64 bits.
		public const UInt64 kTableMagicNumber = 0xdb4775248b80fb57ul;

		// 1-byte type + 32-bit crc
		public const int kBlockTrailerSize = 5;


		public static Status ReadBlock(RandomAccessFile file, ReadOptions options, BlockHandle handle, out BlockContents result)
		{
			result.Data = new Slice();
			result.Cachable = false;
			result.HeapAllocated = false;

			// Read the block contents as well as the type/crc footer.
			// See table_builder.cc for the code that built this structure.
			int n = (int)handle.Size;
			ByteArrayPointer buf = new ByteArrayPointer(n + kBlockTrailerSize);

			Slice contents;
			Status s = file.Read(handle.Offset, n + kBlockTrailerSize, out contents, buf);

			if (!s.IsOk)
			{
				return s;
			}
			if (contents.Size != n + kBlockTrailerSize)
			{
				return Status.Corruption("truncated block read");
			}

			// Check the crc of the type and the block contents
			ByteArrayPointer data = contents.Data;    // Pointer to where Read put the data
			if (options.VerifyChecksums)
			{
				//    const uint32_t crc = crc32c::Unmask(DecodeFixed32(data + n + 1));
				//const uint32_t actual = crc32c::Value(data, n + 1);
				//if (actual != crc) {
				//  delete[] buf;
				//  s = Status::Corruption("block checksum mismatch");
				//  return s;
				//}
				throw new NotSupportedException("CRC is not supported yet");
			}

			switch ((CompressionType)data[n])
			{
				case CompressionType.kNoCompression:
					if (data != buf)
					{
						// File implementation gave us pointer to some other data.
						// Use it directly under the assumption that it will be live
						// while the file is open.

						result.Data = new Slice(data, n);
						result.HeapAllocated = false;
						result.Cachable = false;  // Do not double-cache
					}
					else
					{
						result.Data = new Slice(buf, n);
						result.HeapAllocated = true;
						result.Cachable = true;
					}

					// Ok
					break;
				case CompressionType.kSnappyCompression:
					throw new NotSupportedException("snappy not supported");
				//  {
				//  int ulength = 0;
				//  if (!port::Snappy_GetUncompressedLength(data, n, &ulength)) {
				//    delete[] buf;
				//    return Status::Corruption("corrupted compressed block contents");
				//  }
				//  char* ubuf = new char[ulength];
				//  if (!port::Snappy_Uncompress(data, n, ubuf)) {
				//    delete[] buf;
				//    delete[] ubuf;
				//    return Status::Corruption("corrupted compressed block contents");
				//  }
				//  delete[] buf;
				//  result->data = Slice(ubuf, ulength);
				//  result->heap_allocated = true;
				//  result->cachable = true;
				//  break;
				//}
				default:

					return Status.Corruption("bad block type");
			}

			return Status.OK;

		}
	}
}
