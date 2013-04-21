using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using NetLevelDB.CSharp;

namespace NetLevelDB.Util
{
	internal class BytewiseComparator : Comparator
	{
		public override int Compare(Slice a, Slice b)
		{
			return a.Compare(b);
		}

		public override string Name
		{
			get { return "leveldb.BytewiseComparator"; }
		}

		public override void FindShortestSeparator(ref string start, Slice limit)
		{
			// Find length of common prefix
			int minLength = Math.Min(start.Length, limit.Size);
			int diffIndex = 0;
						
			while ((diffIndex < minLength) && (start[diffIndex].Equals((char)limit[diffIndex])))
			{
				diffIndex++;
			}

			if (diffIndex >= minLength)
			{
				// Do not shorten if one string is a prefix of the other
			}
			else
			{
				byte diffByte = (byte)start[diffIndex];
				if (diffByte < 0xff &&
				    diffByte + 1 < limit[diffIndex])
				{
					start = start.Set(diffIndex,(char) (start[diffIndex] + 1)); 					

					start = start.Resize(diffIndex + 1);
					
					Debug.Assert(Compare(new Slice(start), limit) < 0);					
				}
			}
		}


		public override void FindShortSuccessor(ref string key)
		{
			// Find first character that can be incremented
			int n = key.Length;
			for (int i = 0; i < n; i++)
			{
				if (key[i].Equals((char)0xff))
				{
					key = key.Set(i, (char)(key[i] + 1));

					key = key.Resize(i + 1);
					
					return;
				}
			}

			// *key is a run of 0xffs.  Leave it alone.
		}

	}
}
