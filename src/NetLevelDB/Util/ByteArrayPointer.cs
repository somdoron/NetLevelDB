using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace NetLevelDB.Util
{
	public struct ByteArrayPointer
	{
		private byte[] m_bytes;
		private int m_offset;
		
		public ByteArrayPointer(int size)
		{
			m_bytes = new byte[size];
			m_offset = 0;			
		}

		public ByteArrayPointer(byte[] array, int offset)
		{
			m_bytes = array;
			m_offset = offset;			
		}

		public ByteArrayPointer(byte[] array)
			: this(array, 0)
		{

		}

		public ByteArrayPointer(ByteArrayPointer other)
			: this(other.m_bytes, other.m_offset)
		{

		}

		

		public ByteArrayPointer(ByteArrayPointer other, int offset) :
			this(other.m_bytes, other.m_offset + offset)
		{

		}

		public int Length
		{
			get { return m_bytes.Length - m_offset; }
		}

		public byte[] Data { get { return m_bytes; } }

		public bool IsNull
		{
			get { return m_bytes == null; }
		}

		public byte this[int i]
		{
			get
			{
				if (m_bytes == null)
				{
					throw new NullReferenceException("Pointer not initialized");
				}
				else if (i < 0 || i >= Length)
				{
					throw new ArgumentOutOfRangeException();
				}


				return m_bytes[i + m_offset];
			}
			set
			{
				if (m_bytes == null)
				{
					throw new NullReferenceException("Pointer not initialized");
				}
				else if (i < 0 || i >= Length)
				{
					throw new ArgumentOutOfRangeException();
				}

				m_bytes[i + m_offset] = value;
			}
		}		

		public void CopyTo(int srcOffset, byte[] destArray, int destOffset, int count)
		{
			Buffer.BlockCopy(m_bytes, m_offset + srcOffset, destArray, destOffset, count);
		}

		public void CopyTo(byte[] destArray, int destOffset, int count)
		{
			CopyTo(0, destArray, destOffset, count);
		}

		public void CopyTo(int srcOffset, ByteArrayPointer array, int count)
		{
			CopyTo(srcOffset, array.m_bytes, array.m_offset, count);
		}
		
		public void CopyTo(ByteArrayPointer array, int count)
		{
			CopyTo(0, array, count);
		}

		public ByteArrayPointer Extend(int size, bool setOffsetToLength)
		{
			ByteArrayPointer newPointer = new ByteArrayPointer(Length + size);

			CopyTo(newPointer, Length);

			if (setOffsetToLength)
			{
				newPointer += Length;
			}

			return newPointer;
		}

		public string GetString()
		{
			return GetString(Encoding.ASCII);
		}

		public string GetString(Encoding encoding)
		{
			return encoding.GetString(m_bytes, m_offset, Length);
		}

		public static ByteArrayPointer Null
		{
			get
			{
				return new ByteArrayPointer(null);
			}
		}

		public static ByteArrayPointer operator +(ByteArrayPointer byteArrayPointer, int n)
		{
			return new ByteArrayPointer(byteArrayPointer, n);
		}

		public static ByteArrayPointer operator -(ByteArrayPointer byteArrayPointer, int n)
		{
			return new ByteArrayPointer(byteArrayPointer, n *-1);
		}

		public static int operator +(ByteArrayPointer b1, ByteArrayPointer b2)
		{
			if (b1.m_bytes != b2.m_bytes)
			{
				throw new ArgumentException("Pointers point to diffrent arrays");
			}

			return b1.m_offset + b2.m_offset;
		}

		public static int operator -(ByteArrayPointer b1, ByteArrayPointer b2)
		{
			if (b1.m_bytes != b2.m_bytes)
			{
				throw new ArgumentException("Pointers point to diffrent arrays");
			}

			return b1.m_offset - b2.m_offset;
		}

		public static bool operator <(ByteArrayPointer b1, ByteArrayPointer b2)
		{
			if (b1.m_bytes != b2.m_bytes)
			{
				throw new ArgumentException("Pointers point to diffrent arrays");
			}

			return b1.m_offset < b2.m_offset;
		}

		public static bool operator >(ByteArrayPointer b1, ByteArrayPointer b2)
		{
			if (b1.m_bytes != b2.m_bytes)
			{
				throw new ArgumentException("Pointers point to diffrent arrays");
			}

			return b1.m_offset > b2.m_offset;
		}

		public static bool operator <=(ByteArrayPointer b1, ByteArrayPointer b2)
		{
			if (b1.m_bytes != b2.m_bytes)
			{
				throw new ArgumentException("Pointers point to diffrent arrays");
			}

			return b1.m_offset <= b2.m_offset;
		}

		public static bool operator >=(ByteArrayPointer b1, ByteArrayPointer b2)
		{
			if (b1.m_bytes != b2.m_bytes)
			{
				throw new ArgumentException("Pointers point to diffrent arrays");
			}

			return b1.m_offset >= b2.m_offset;
		}

		public static bool operator ==(ByteArrayPointer b1, ByteArrayPointer b2)
		{		
			return b1.m_bytes == b2.m_bytes && b1.m_offset == b2.m_offset;
		}

		public static bool operator !=(ByteArrayPointer b1, ByteArrayPointer b2)
		{
			return !(b1 == b2);
		}		
	}
}
