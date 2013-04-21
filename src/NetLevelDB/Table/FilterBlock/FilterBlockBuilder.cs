using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using NetLevelDB.Util;

namespace NetLevelDB.Table.FilterBlock
{
	class FilterBlockBuilder
	{
		const char kFilterBaseLg = (char)11;
		const int kFilterBase = 1 << (int)kFilterBaseLg;

		FilterPolicy policy_;
		string keys_;              // Flattened key contents
		IList<int> start_;     // Starting index in keys_ of each key
		string result_;            // Filter data computed so far
		IList<Slice> tmp_keys_;   // policy_->CreateFilter() argument
		IList<UInt32> filter_offsets_;


		public FilterBlockBuilder(FilterPolicy policy)
		{
			start_ = new List<int>();
			tmp_keys_ = new List<Slice>();
			filter_offsets_ = new List<uint>();
			policy_ = policy;
		}

		public void StartBlock(UInt64 block_offset)
		{
			UInt64 filter_index = (block_offset / kFilterBase);
			Debug.Assert(filter_index >= (ulong)filter_offsets_.Count);
			while (filter_index > (ulong)filter_offsets_.Count)
			{
				GenerateFilter();
			}
		}

		public void AddKey(Slice key)
		{
			Slice k = key.Clone();
			start_.Add(keys_.Length);
			keys_ += k.Data.GetString(k.Size);
		}

		public Slice Finish()
		{
			if (start_.Count != 0)
			{
				GenerateFilter();
			}

			// Append array of per-filter offsets
			UInt32 array_offset = (uint)result_.Length;
			for (int i = 0; i < filter_offsets_.Count; i++)
			{
				Coding.PutFixed32(ref result_, filter_offsets_[i]);
			}

			Coding.PutFixed32(ref result_, array_offset);
			result_ += kFilterBaseLg;  // Save encoding parameter in result
			return new Slice(result_);
		}


		void GenerateFilter()
		{
			int num_keys = start_.Count;
  if (num_keys == 0) {
    // Fast path if there are no keys for this filter
    filter_offsets_.Add((uint)result_.Length);
    return;
  }

  // Make list of keys from flattened key structure
  start_.Add(keys_.Length);  // Simplify length computation
  //tmp_keys_.resize(num_keys);
  for (int i = 0; i < num_keys; i++) {
    var b = keys_.Substring(start_[i]);
    int length = start_[i+1] - start_[i];
    tmp_keys_.Add(new Slice(b.Substring(0,length)));
  }

  // Generate filter for current set of keys and append to result_.
  filter_offsets_.Add((uint)result_.Length);
  policy_.CreateFilter(tmp_keys_.ToArray(), num_keys, ref result_);

  tmp_keys_.Clear();
			keys_ = string.Empty;
  start_.Clear();
		}
	}
}
