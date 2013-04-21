using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace NetLevelDB.Table.Format
{
	struct BlockContents
	{
		public Slice Data;           // Actual contents of data
		public bool Cachable;        // True iff data can be cached
		public bool HeapAllocated;  // True iff caller should delete[] data.data()
	}
}
