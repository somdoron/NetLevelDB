using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;

namespace NetLevelDB.Port
{
	public class AtomicInteger
	{
		private int m_rep;

		public AtomicInteger(int rep)
		{
			m_rep = rep;
		}

		public int NoBarrierLoad() { return m_rep; }

		public void NoBarrierStore(int v) { m_rep = v; }

		public int AcquireLoad()
		{
			int result = m_rep;
			Thread.MemoryBarrier();
			return result;
		}

		public void ReleaseStore(int v)
		{
			Thread.MemoryBarrier();
			m_rep = v;
		}
	}
}
