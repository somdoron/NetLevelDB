using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NetLevelDB.CSharp;

namespace NetLevelDB.Util
{
	public static class Coding
	{
		public static void EncodeFixed32(ByteArrayPointer buf, UInt32 value)
		{
			buf[0] = (byte)(value & 0xff);
			buf[1] = (byte)((value >> 8) & 0xff);
			buf[2] = (byte)((value >> 16) & 0xff);
			buf[3] = (byte)((value >> 24) & 0xff);
		}

		public static void EncodeFixed64(ByteArrayPointer buf, UInt64 value)
		{
			buf[0] = (byte)(value & 0xff);
			buf[1] = (byte)((value >> 8) & 0xff);
			buf[2] = (byte)((value >> 16) & 0xff);
			buf[3] = (byte)((value >> 24) & 0xff);
			buf[4] = (byte)((value >> 32) & 0xff);
			buf[5] = (byte)((value >> 40) & 0xff);
			buf[6] = (byte)((value >> 48) & 0xff);
			buf[7] = (byte)((value >> 56) & 0xff);
		}

		public static void PutFixed32(ref string dst, UInt32 value)
		{
			ByteArrayPointer buf = new ByteArrayPointer(sizeof(UInt32));
			EncodeFixed32(buf, value);

			dst += buf.GetString(buf.Length);
		}

		public static void PutFixed64(ref string dst, UInt64 value)
		{
			ByteArrayPointer buf = new ByteArrayPointer(sizeof(UInt64));
			EncodeFixed64(buf, value);

			dst += buf.GetString(buf.Length);
		}

		public static ByteArrayPointer EncodeVarint32(ByteArrayPointer dst, UInt32 v)
		{
			// Operate on characters as unsigneds
			ByteArrayPointer ptr = dst;
			int B = 128;
			if (v < (1 << 7))
			{
				ptr[0] = (byte)v;
				ptr += 1;
			}
			else if (v < (1 << 14))
			{
				ptr[0] = (byte)(v | B);
				ptr[1] = (byte)(v >> 7);
				ptr += 2;
			}
			else if (v < (1 << 21))
			{
				ptr[0] = (byte)(v | B);
				ptr[1] = (byte)((v >> 7) | B);
				ptr[2] = (byte)(v >> 14);
				ptr += 3;
			}
			else if (v < (1 << 28))
			{
				ptr[0] = (byte)(v | B);
				ptr[1] = (byte)((v >> 7) | B);
				ptr[2] = (byte)((v >> 14) | B);
				ptr[3] = (byte)(v >> 21);
				ptr += 4;
			}
			else
			{
				ptr[0] = (byte)(v | B);
				ptr[1] = (byte)((v >> 7) | B);
				ptr[2] = (byte)((v >> 14) | B);
				ptr[3] = (byte)((v >> 21) | B);
				ptr[4] = (byte)(v >> 28);
				ptr += 5;
			}

			return ptr;
		}

		public static void PutVarint32(ref string dst, UInt32 v)
		{
			ByteArrayPointer buf = new ByteArrayPointer(5);
			ByteArrayPointer ptr = EncodeVarint32(buf, v);
			dst += buf.GetString(ptr - buf);
		}

		public static ByteArrayPointer EncodeVarint64(ByteArrayPointer dst, UInt64 v)
		{
			const int B = 128;
			ByteArrayPointer ptr = dst;
			while (v >= B)
			{
				ptr[0] = (byte)((v & (B - 1)) | B);
				ptr += 1;
				v >>= 7;
			}
			ptr[0] = (byte)v;
			ptr += 1;
			return ptr;
		}

		public static void PutVarint64(ref string dst, UInt64 v)
		{
			ByteArrayPointer buf = new ByteArrayPointer(10);
			ByteArrayPointer ptr = EncodeVarint64(buf, v);
			dst += buf.GetString(ptr - buf);
		}

		public static void PutLengthPrefixedSlice(ref string dst, Slice value)
		{
			PutVarint32(ref dst, (uint)value.Size);
			dst += value.Data.GetString(value.Size);
		}


		public static int VarintLength(int v)
		{
			return VarintLength((UInt64)v);
		}

		public static int VarintLength(UInt64 v)
		{
			int len = 1;
			while (v >= 128)
			{
				v >>= 7;
				len++;
			}
			return len;
		}

		public static ByteArrayPointer GetVarint32PtrFallback(ByteArrayPointer p,
																													ByteArrayPointer limit,
																													out UInt32 value)
		{
			UInt32 result = 0;
			for (int shift = 0; shift <= 28 && p < limit; shift += 7)
			{
				UInt32 b = p[0];
				p += 1;
				if ((b & 128) != 0)
				{
					// More bytes are present
					result |= ((b & 127) << shift);
				}
				else
				{
					result |= (b << shift);
					value = result;
					return p;
				}
			}
			value = 0;
			return ByteArrayPointer.Null;
		}

		public static bool GetVarint32(Slice input, out UInt32 value)
		{
			ByteArrayPointer p = input.Data;
			ByteArrayPointer limit = p + input.Size;
			ByteArrayPointer q = GetVarint32Ptr(p, limit, out value);
			if (q.IsNull)
			{
				return false;
			}
			else
			{
				input = new Slice(q, limit - q);
				return true;
			}
		}

		public static ByteArrayPointer GetVarint64Ptr(ByteArrayPointer p, ByteArrayPointer limit, out UInt64 value)
		{
			UInt64 result = 0;
			for (int shift = 0; shift <= 63 && p < limit; shift += 7)
			{
				UInt64 b = p[0];
				p += 1;

				if ((b & 128) != 0)
				{
					// More bytes are present
					result |= ((b & 127) << shift);
				}
				else
				{
					result |= (b << shift);
					value = result;
					return p;
				}
			}

			value = 0;
			return ByteArrayPointer.Null;
		}

		public static bool GetVarint64(ref Slice input, out UInt64 value)
		{
			ByteArrayPointer p = input.Data;
			ByteArrayPointer limit = p + input.Size;
			ByteArrayPointer q = GetVarint64Ptr(p, limit, out value);

			if (q.IsNull)
			{
				return false;
			}
			else
			{
				input = new Slice(q, limit - q);
				return true;
			}
		}

		public static ByteArrayPointer GetLengthPrefixedSlice(ByteArrayPointer p, ByteArrayPointer limit,
																			 out Slice result)
		{
			result = null;
			UInt32 len;
			p = GetVarint32Ptr(p, limit, out len);
			if (p.IsNull) return p;
			if (p + (int)len > limit) return ByteArrayPointer.Null;
			result = new Slice(p, (int)len);

			return p + (int)len;
		}

		public static bool GetLengthPrefixedSlice(Slice input, out Slice result)
		{
			UInt32 len;
			if (GetVarint32(input, out len) &&
					input.Size >= len)
			{
				result = new Slice(input.Data,(int) len);
				input.RemovePrefix((int)len);
				return true;
			}
			else
			{
				result = null;
				return false;
			}
		}

		public static UInt32 DecodeFixed32(ByteArrayPointer ptr)
		{

			return ((UInt32)ptr[0])
					| (((UInt32)ptr[1]) << 8)
					| (((UInt32)ptr[2]) << 16)
					| (((UInt32)ptr[3]) << 24);
		}
		
		public static UInt64 DecodeFixed64(ByteArrayPointer ptr)
		{
			UInt64 lo = DecodeFixed32(ptr);
			UInt64 hi = DecodeFixed32(ptr + 4);
			return (hi << 32) | lo;
		}

		public static ByteArrayPointer GetVarint32Ptr(ByteArrayPointer p,
																									ByteArrayPointer limit,
																									out UInt32 value)
		{
			if (p < limit)
			{
				UInt32 result = p[0];
				if ((result & 128) == 0)
				{
					value = result;
					return new ByteArrayPointer(p, 1);
				}
			}
			return GetVarint32PtrFallback(p, limit, out value);
		}


		


				
	}


}
