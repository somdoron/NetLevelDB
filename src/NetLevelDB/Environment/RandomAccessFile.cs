using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NetLevelDB.CSharp;
using NetLevelDB.Table.Format;
using NetLevelDB.Util;

namespace NetLevelDB.Environment
{
	public abstract class RandomAccessFile
	{
		public RandomAccessFile()
		{
			
		}

		public abstract Status Read(UInt64 offset, int n, out Slice result, ByteArrayPointer scratch);
		
		
	}
}
