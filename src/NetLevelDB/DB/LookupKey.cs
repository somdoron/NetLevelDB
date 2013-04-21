using System;
using NetLevelDB.CSharp;
using NetLevelDB.Util;

namespace NetLevelDB.DB
{
	class LookupKey
	{
		// We construct a char array of the form:
		//    klength  varint32               <-- start_
		//    userkey  char[klength]          <-- kstart_
		//    tag      uint64
		//                                    <-- end_
		// The array is a suitable MemTable key.
		// The suffix starting with "userkey" can be used as an InternalKey.
		ByteArrayPointer start_;
		ByteArrayPointer kstart_;
		ByteArrayPointer end_;
		byte[] space_ = new byte[200];      // Avoid allocation for short keys

		

		// Initialize *this for looking up user_key at a snapshot with
		// the specified sequence number.
		public LookupKey(Slice user_key, UInt64 sequence)
		{
			int usize = user_key.Size;
			int needed = usize + 13;  // A conservative estimate
			ByteArrayPointer dst;
			
			if (needed <= space_.Length)
			{
				dst = new ByteArrayPointer(space_);
			}
			else
			{
				dst = new ByteArrayPointer(needed);
			}

			start_ = dst;
			dst = Coding.EncodeVarint32(dst, (uint)(usize + 8));
			kstart_ = dst;

			user_key.Data.CopyTo(dst, usize);

			dst += usize;

			Coding.EncodeFixed64(dst, Global.PackSequenceAndType(sequence ,Global.kValueTypeForSeek));
			end_ = dst + 8;
		}

		// Return a key suitable for lookup in a MemTable.
		public Slice GetMemTableKey()
		{ return new Slice(start_, end_ - start_); }

		// Return an internal key (suitable for passing to an internal iterator)
		public Slice GetInternalKey()
		{
			return new Slice(kstart_, end_ - kstart_);
		}

		// Return the user key
		public Slice GetUserKey()
		{ return new Slice(kstart_, end_ - kstart_ - 8); }
	}
}
