﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace NetLevelDB
{
	// Options that control write operations
	public class WriteOptions
	{
		  // If true, the write will be flushed from the operating system
  // buffer cache (by calling WritableFile::Sync()) before the write
  // is considered complete.  If this flag is true, writes will be
  // slower.
  //
  // If this flag is false, and the machine crashes, some recent
  // writes may be lost.  Note that if it is just the process that
  // crashes (i.e., the machine does not reboot), no writes will be
  // lost even if sync==false.
  //
  // In other words, a DB write with sync==false has similar
  // crash semantics as the "write()" system call.  A DB write
  // with sync==true has similar crash semantics to a "write()"
  // system call followed by "fsync()".
  //
  // Default: false
  public bool Sync { get; set; }

  public WriteOptions()
  {
	  Sync = false;
  }
  
	}
}
