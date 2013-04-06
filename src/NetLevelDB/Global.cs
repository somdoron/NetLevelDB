using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;

namespace NetLevelDB
{
	public enum ValueTypeEnum {
			kTypeDeletion = 0x0,
			kTypeValue = 0x1
	};

	public static class Global
	{		
			// kValueTypeForSeek defines the ValueType that should be passed when
			// constructing a ParsedInternalKey object for seeking to a particular
			// sequence number (since we sort sequence numbers in decreasing order
			// and the value type is embedded as the low 8 bits in the sequence
			// number in internal keys, we need to use the highest-numbered
			// ValueType, not the lowest).
			public const ValueTypeEnum kValueTypeForSeek = ValueTypeEnum.kTypeValue;

			// We leave eight bits empty at the bottom so a type and sequence#
			// can be packed together into 64-bits.
			public const UInt64 kMaxSequenceNumber = ((0x1ul << 56) - 1);


			public static UInt64 PackSequenceAndType(UInt64 seq, ValueTypeEnum t)
			{
				Debug.Assert(seq <= kMaxSequenceNumber);
				Debug.Assert(t <= kValueTypeForSeek);
				return (seq << 8) | (UInt64)t;
			}
	}
}
