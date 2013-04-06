using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace NetLevelDB
{
	/// <summary>
	/// Abstract handle to particular state of a DB.
	/// A Snapshot is an immutable object and can therefore be safely
	/// accessed from multiple threads without any external synchronization.	
	/// </summary>
	public abstract class Snapshot
	{
	}
}
