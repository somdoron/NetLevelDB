using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace NetLevelDB
{
	// Options that control read operations
	public class ReadOptions
	{
		// If true, all data read from underlying storage will be
		// verified against corresponding checksums.
		// Default: false
		public bool VerifyChecksums { get; set; }

		// Should the data read for this iteration be cached in memory?
		// Callers may wish to set this field to false for bulk scans.
		// Default: true
		public bool FillCache { get; set; }

		// If "snapshot" is non-NULL, read as of the supplied snapshot
		// (which must belong to the DB that is being read and which must
		// not have been released).  If "snapshot" is NULL, use an impliicit
		// snapshot of the state at the beginning of this read operation.
		// Default: NULL
		public Snapshot Snapshot { get; set; }

		public ReadOptions()
		{
			VerifyChecksums = false;
			FillCache = true;
			Snapshot = null;
		}

		public static ReadOptions Default
		{
			get
			{
				return new ReadOptions();
			}
		}
	}
}
