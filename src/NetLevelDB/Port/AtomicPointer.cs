using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;

namespace NetLevelDB.Port
{
	public class AtomicPointer<T>
	{
		private T m_rep;

		public AtomicPointer(T rep)
		{
			m_rep = rep;
		}

		public T NoBarrierLoad() { return m_rep; }
		
		public void NoBarrierStore(T v) { m_rep = v; }
		
		public T AcquireLoad()
		{
			T result = m_rep;
			Thread.MemoryBarrier();
			return result;
		}

		public void ReleaseStore(T v)
		{
			Thread.MemoryBarrier();
			m_rep = v;
		}
	}
}
