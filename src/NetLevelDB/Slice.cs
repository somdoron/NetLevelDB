using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using NetLevelDB.Util;

namespace NetLevelDB
{
	public class Slice : IEquatable<Slice>
	{
		// Create an empty slice.
		public Slice()
		{
			Data = new ByteArrayPointer(0);
			Size = 0;
		}		

		public Slice(ByteArrayPointer d) : this(d,d.Length)
		{
			
		}		

		public Slice(ByteArrayPointer d, int n)
		{
			Data = d;
			Size = n;
		}

		public ByteArrayPointer Data { get; private set; }

		public int Size { get; private set; }

		// Return true iff the length of the referenced data is zero
		public bool Empty
		{
			get { return Size == 0; }
		}

		// Return the ith byte in the referenced data.
		// REQUIRES: n < size()
		public byte this[int n]
		{
			get
			{
				Debug.Assert(n < Size);
				return Data[n];
			}
		}

		// Change this slice to refer to an empty array
		public void Clear()
		{
			Data = new ByteArrayPointer(0);			
		}

		// Drop the first "n" bytes from this slice.
		public void RemovePrefix(int n)
		{
			Debug.Assert(n <= Size);
			Data += n;
			Size -= n;
		}

		public override string ToString()
		{
			return Data.GetString();
		}

		// Three-way comparison.  Returns value:
		//   <  0 iff "*this" <  "b",
		//   == 0 iff "*this" == "b",
		//   >  0 iff "*this" >  "b"
		public int Compare(Slice b)
		{
			int min_len = (Size < b.Size) ? Size : b.Size;
			int r = 0;

			for (int i = 0; i < min_len; i++)
			{
				if (this[i] != b[i])
				{
					r = this[i].CompareTo(b[i]);
			
					break;
				}
			}							

			if (r == 0)
			{
				if (Size < b.Size) r = -1;
				else if (Size > b.Size) r = +1;
			}
			return r;
		}

		// Return true iff "x" is a prefix of "*this"
		public bool StartsWith(Slice x)
		{
			if (Size >= x.Size)
			{
				for (int i = 0; i < x.Size; i++)
				{
					if (this[i] != x[i])
					{
						return false;
					}	
				}

				return true;
			}

			return false;
		}

		public Slice ExtractUserKey() 
		{
			Debug.Assert(Size >= 8);
			return new Slice(Data, Size - 8);
	}


		public bool Equals(Slice other)
		{
			if (ReferenceEquals(null, other)) return false;
			if (ReferenceEquals(this, other)) return true;

			if ((Size == other.Size))
			{
				for (int i = 0; i < Size; i++)
				{
					if (this[i] != other[i])
					{
						return false;
					}
				}

				return true;
			}

			return false;
		}

		public override bool Equals(object obj)
		{
			if (ReferenceEquals(null, obj)) return false;
			if (ReferenceEquals(this, obj)) return true;
			if (obj.GetType() != this.GetType()) return false;
			return Equals((Slice) obj);
		}

		public override int GetHashCode()
		{
			throw new NotImplementedException();
			return (Data != null ? Data.GetHashCode() : 0);
		}

		public static bool operator ==(Slice left, Slice right)
		{
			return Equals(left, right);
		}

		public static bool operator !=(Slice left, Slice right)
		{
			return !Equals(left, right);
		}		
	}
}

