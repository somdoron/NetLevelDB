using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace NetLevelDB.Util
{
	public class Cache
	{
		public class Handle
		{
			
		}

		internal ulong NewId()
		{
			throw new NotImplementedException();
		}

		internal void Release(Handle handle)
		{
			throw new NotImplementedException();
		}

		internal Handle Lookup(Slice key)
		{
			throw new NotImplementedException();
		}

		internal object Value(Handle cacheHandle)
		{
			throw new NotImplementedException();
		}

		internal Handle Insert(Slice key, Table.Block block, int p)
		{
			throw new NotImplementedException();
		}
	}
}
