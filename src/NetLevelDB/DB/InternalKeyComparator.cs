using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using NetLevelDB.CSharp;
using NetLevelDB.Util;

namespace NetLevelDB.DB
{
	class InternalKeyComparator : Comparator
	{
		Comparator m_userComparator;

		public Comparator UserComparator
		{
			get { return m_userComparator; }
		}

		public InternalKeyComparator(Comparator c)
		{
			m_userComparator = c;
		}

		public override int Compare(Slice akey, Slice bkey)
		{
			// Order by:
			//    increasing user key (according to user-supplied comparator)
			//    decreasing sequence number
			//    decreasing type (though sequence# should be enough to disambiguate)
			int r = UserComparator.Compare(akey.ExtractUserKey(), bkey.ExtractUserKey());
			if (r == 0)
			{
				UInt64 anum = Coding.DecodeFixed64(akey.Data + (akey.Size - 8));
				UInt64 bnum = Coding.DecodeFixed64(bkey.Data + (bkey.Size - 8));
				if (anum > bnum)
				{
					r = -1;
				}
				else if (anum < bnum)
				{
					r = +1;
				}
			}
			return r;
		}

		public override string Name
		{
			get { return "leveldb.InternalKeyComparator"; }
		}

		public override void FindShortestSeparator(ref string start, Slice limit)
		{
			// Attempt to shorten the user portion of the key
			Slice user_start = new Slice(start).ExtractUserKey();
			Slice user_limit = limit.ExtractUserKey();
			string tmp = user_start.Data.GetString(user_start.Size);
			
			UserComparator.FindShortestSeparator(ref tmp, user_limit);
			if (tmp.Length < user_start.Size &&
					UserComparator.Compare(user_start, new Slice(tmp)) < 0)
			{				
				// User key has become shorter physically, but larger logically.
				// Tack on the earliest possible number to the shortened user key.

				Coding.PutFixed64(ref tmp,
					Global.PackSequenceAndType(Global.kMaxSequenceNumber, Global.kValueTypeForSeek));

				Debug.Assert(this.Compare(new Slice(start), new Slice(tmp)) < 0);
				Debug.Assert(this.Compare(new Slice(tmp), limit) < 0);
				start = start.Swap(ref tmp);
			}
		}

		public override void FindShortSuccessor(ref string key)
		{
			Slice user_key = new Slice(key).ExtractUserKey();
			string tmp = user_key.Data.GetString(user_key.Size);

			UserComparator.FindShortSuccessor(ref tmp);
			if (tmp.Length < user_key.Size &&
					UserComparator.Compare(user_key, new Slice(tmp)) < 0)
			{
				// User key has become shorter physically, but larger logically.
				// Tack on the earliest possible number to the shortened user key.

				
				Coding.PutFixed64(ref tmp, Global.PackSequenceAndType(Global.kMaxSequenceNumber, Global.kValueTypeForSeek));
				Debug.Assert(this.Compare(new Slice(key), new Slice(tmp)) < 0);
				key = tmp;
			}
		}
	}
}
