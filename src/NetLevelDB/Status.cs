using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;

namespace NetLevelDB
{
	public class Status
	{
		enum CodeEnum : byte
		{
			kOk = 0,
			kNotFound = 1,
			kCorruption = 2,
			kNotSupported = 3,
			kInvalidArgument = 4,
			kIOError = 5
		}

		private byte[] m_state;

		public Status()
		{
			m_state = null;
		}


		private Status(CodeEnum code, Slice msg, Slice msg2)
		{			
			Debug.Assert(code != CodeEnum.kOk);
			int len1 = msg.Size;

			int len2 = 0;
			
			if (msg2 != null)
			{
				len2 = msg2.Size;
			}
			
			int size = len1 + (len2 != 0 ? (2 + len2) : 0);
			byte[] result = new byte[size + 5];
			
			Buffer.BlockCopy(BitConverter.GetBytes(size), 0, result, 0, 4);

			result[4] = (byte) Code;

			msg.Data.CopyTo(result,5, len1);
			
			if (len2!=0)
			{
				result[5 + len1] = (byte)':';
				result[6 + len1] = (byte)' ';

				msg2.Data.CopyTo(result, 7 + len1, len2);				
			}

			m_state = result;
		}


		public Status(Status s)
		{
			m_state = (s.m_state == null) ? null : CopyState(s.m_state);
		}

		private CodeEnum Code
		{
			get { return (m_state == null) ? CodeEnum.kOk : (CodeEnum)(m_state[4]); }
		}

		private static byte[] CopyState(byte[] state)
		{
			int size = BitConverter.ToInt32(state, 0);
			
			byte[] result = new byte[size + 5];

			Buffer.BlockCopy(state, 0, result, 0, 5);				
			
			return result;
		}

		// Return a success status.
		public static Status OK
		{
			get { return new Status(); }
		}

		// Return error status of an appropriate type.
		public static Status NotFound(Slice msg, Slice msg2 = null)
		{
			return new Status(CodeEnum.kNotFound, msg, msg2);
		}

		public static Status Corruption(Slice msg, Slice msg2 = null)
		{
			return new Status(CodeEnum.kCorruption, msg, msg2);
		}
		public static Status NotSupported(Slice msg, Slice msg2 = null)
		{
			return new Status(CodeEnum.kNotSupported, msg, msg2);
		}
		public static Status InvalidArgument(Slice msg, Slice msg2 = null)
		{
			return new Status(CodeEnum.kInvalidArgument, msg, msg2);
		}
		public static Status IOError(Slice msg, Slice msg2 = null)
		{
			return new Status(CodeEnum.kIOError, msg, msg2);
		}

		// Returns true iff the status indicates success.
		public bool IsOk { get { return (m_state == null); } }

		// Returns true iff the status indicates a NotFound error.
		public bool IsNotFound { get { return Code == CodeEnum.kNotFound; } }

		// Returns true iff the status indicates a Corruption error.
		public bool IsCorruption { get { return Code == CodeEnum.kCorruption; } }

		// Returns true iff the status indicates an IOError.
		public bool IsIOError
		{
			get { return Code == CodeEnum.kIOError; }
		}

		// Return a string representation of this status suitable for printing.
		// Returns the string "OK" for success.

		public override string ToString()
		{
			if (m_state == null)
			{
				return "OK";
			}
			else
			{
				string type;
				switch (Code)
				{
					case CodeEnum.kOk:
						type = "OK";
						break;
					case CodeEnum.kNotFound:
						type = "NotFound: ";
						break;
					case CodeEnum.kCorruption:
						type = "Corruption: ";
						break;
					case CodeEnum.kNotSupported:
						type = "Not implemented: ";
						break;
					case CodeEnum.kInvalidArgument:
						type = "Invalid argument: ";
						break;
					case CodeEnum.kIOError:
						type = "IO error: ";
						break;
					default:
						type = string.Format("Unknown code({0}): ", Code);

						break;
				}

				int length = BitConverter.ToInt32(m_state, 0);

				return type + Encoding.ASCII.GetString(m_state, 5, length);				
			}
		}
	}
}
