using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace NetLevelDB.CSharp
{
	public static class StringExtension
	{
		public static string Resize(this string s, int length)
		{
			if (s.Length > length)
			{
				return s.PadRight(s.Length - length);
			}
			else
			{
				return s.Substring(0, length);
			}
		}

		//public static string Append(this string s, string text, int length)
		//{
		//  s += text.Substring(0, length);

		//  return s;
		//}

		public static string Set(this string s, int index, char c)
		{
			var chars = s.ToCharArray();
			chars[index] = c;

			return new string(chars);
		}

		public static string Swap(this string s, ref string other)
		{
			string tmp = other;
			other = s;

			return tmp;
		}
	}
}
