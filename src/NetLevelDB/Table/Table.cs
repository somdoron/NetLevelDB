using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NetLevelDB.CSharp;
using NetLevelDB.Environment;
using NetLevelDB.Table.FilterBlock;
using NetLevelDB.Table.Format;
using NetLevelDB.Util;

namespace NetLevelDB.Table
{
	public delegate void HandleResultDelegate(object arg, Slice k, Slice v);

	public class Table
	{
		class Rep
		{
			public Options Options;
			public Status Status;
			public RandomAccessFile File;
			public UInt64 CacheId;
			public FilterBlockReader Filter;
			public ByteArrayPointer FilterData;

			public BlockHandle MetaindexHandle; // Handle to metaindex_block: saved from footer
			public Block IndexBlock;
		}

		Rep rep_;

		private Table(Rep rep)
		{ rep_ = rep; }

		private static Iterator BlockReader(object arg, ReadOptions options, Slice indexValue)
		{
			Table table = (Table)arg;
			Cache blockCache = table.rep_.Options.BlockCache;
			Block block = null;
			Cache.Handle cacheHandle = null;

			BlockHandle handle = new BlockHandle();
			Slice input = indexValue;
			Status s = handle.DecodeFrom(ref input);
			// We intentionally allow extra stuff in index_value so that we
			// can add more features in the future.

			if (s.IsOk)
			{
				BlockContents contents;
				if (blockCache != null)
				{
					ByteArrayPointer cacheKeyBuffer = new ByteArrayPointer(16);

					Coding.EncodeFixed64(cacheKeyBuffer, table.rep_.CacheId);
					Coding.EncodeFixed64(cacheKeyBuffer + 8, handle.Offset);
					
					Slice key = new Slice(cacheKeyBuffer, cacheKeyBuffer.Length);

					cacheHandle = blockCache.Lookup(key);
					if (cacheHandle != null)
					{
						block = (Block)(blockCache.Value(cacheHandle));
					}
					else
					{
						s = FormatHelper.ReadBlock(table.rep_.File, options, handle, out contents);
						if (s.IsOk)
						{
							block = new Block(contents);
							if (contents.Cachable && options.FillCache)
							{
								cacheHandle = blockCache.Insert(key, block, block.Size);
							}
						}
					}
				}
				else
				{
					s = FormatHelper.ReadBlock(table.rep_.File, options, handle, out contents);
					if (s.IsOk)
					{
						block = new Block(contents);
					}
				}
			}

			Iterator iter;

			if (block != null)
			{
				iter = block.NewIterator(table.rep_.Options.Comparator);
				if (cacheHandle != null)
				{
					iter.RegisterCleanup(ReleaseBlock, blockCache, cacheHandle);
				}
			}
			else
			{
				iter = Iterator.NewErrorIterator(s);
			}
			return iter;
		}

		internal Status InternalGet(
			ReadOptions options, Slice k,
			object arg, HandleResultDelegate saver)
		{
			Status s = new Status();
			Iterator iiter = rep_.IndexBlock.NewIterator(rep_.Options.Comparator);
			iiter.Seek(k);
			if (iiter.Valid)
			{
				Slice handle_value = iiter.Value;
				FilterBlockReader filter = rep_.Filter;
				BlockHandle handle = new BlockHandle();
				if (filter != null &&
						handle.DecodeFrom(ref handle_value).IsOk &&
						!filter.KeyMayMatch(handle.Offset, k))
				{
					// Not found
				}
				else
				{
					Slice tempHandle = iiter.Value;
					Iterator blockIter = BlockReader(this, options, iiter.Value);
					blockIter.Seek(k);
					if (blockIter.Valid)
					{
						saver(arg, blockIter.Key, blockIter.Value);						
					}
					s = blockIter.Status;					
				}
			}
			if (s.IsOk)
			{
				s = iiter.Status;
			}
		
			return s;
		}


		void ReadMeta(Footer footer)
		{
			if (rep_.Options.FilterPolicy == null)
			{
				// Do not need any metadata
				return;
			}

			// TODO(sanjay): Skip this if footer.metaindex_handle() size indicates
			// it is an empty block.
			ReadOptions opt = ReadOptions.Default;
			BlockContents contents;
			if (!FormatHelper.ReadBlock(rep_.File, opt, footer.MetaindexHandle, out contents).IsOk)
			{
				// Do not propagate errors since meta info is not needed for operation
				return;
			}

			Block meta = new Block(contents);

			Iterator iter = meta.NewIterator(Comparator.BytewiseComparator);
			string key = "filter." + rep_.Options.FilterPolicy.Name;

			iter.Seek(key);
			if (iter.Valid && iter.Key == key)
			{
				ReadFilter(iter.Value);
			}
		}

		void ReadFilter(Slice filterHandleValue)
		{
			Slice v = filterHandleValue;
			BlockHandle filterHandle = new BlockHandle();
			if (!filterHandle.DecodeFrom(ref v).IsOk)
			{
				return;
			}

			// We might want to unify with ReadBlock() if we start
			// requiring checksum verification in Table::Open.
			ReadOptions opt = ReadOptions.Default;
			BlockContents block;
			if (!FormatHelper.ReadBlock(rep_.File, opt, filterHandle, out block).IsOk)
			{
				return;
			}
			if (block.HeapAllocated)
			{
				rep_.FilterData = block.Data.Data; // Will need to delete later
			}
			rep_.Filter = new FilterBlockReader(rep_.Options.FilterPolicy, block.Data);

		}

		/// <summary>				
		/// Attempt to open the table that is stored in bytes [0..file_size)
		/// of "file", and read the metadata entries necessary to allow
		/// retrieving data from the table.
		///
		/// If successful, returns ok and sets "*table" to the newly opened
		/// table.  The client should delete "*table" when no longer needed.
		/// If there was an error while initializing the table, sets "*table"
		/// to NULL and returns a non-ok status.  Does not take ownership of
		/// "*source", but the client must ensure that "source" remains live
		/// for the duration of the returned table's lifetime.
		///
		/// *file must remain live while this Table is in use.				
		/// </summary>
		public static Status Open(Options options, RandomAccessFile file, UInt64 size, out Table table)
		{
			table = null;
			if (size < Footer.kEncodedLength)
			{
				return Status.InvalidArgument("file is too short to be an sstable");
			}

			ByteArrayPointer footerSpace = new ByteArrayPointer(Footer.kEncodedLength);				

			Slice footerInput;
			Status s = file.Read(size - Footer.kEncodedLength, Footer.kEncodedLength, out footerInput, footerSpace);
			if (!s.IsOk) return s;

			Footer footer = new Footer();
			s = footer.DecodeFrom(ref footerInput);
			if (!s.IsOk) return s;

			// Read the index block
			BlockContents contents;
			Block indexBlock = null;
			if (s.IsOk)
			{
				s = FormatHelper.ReadBlock(file, ReadOptions.Default, footer.IndexHandle, out contents);
				if (s.IsOk)
				{
					indexBlock = new Block(contents);
				}
			}

			if (s.IsOk)
			{
				// We've successfully read the footer and the index block: we're
				// ready to serve requests.

				Rep rep = new Rep();
				rep.Options = options;
				rep.File = file;
				rep.MetaindexHandle = footer.MetaindexHandle;
				rep.IndexBlock = indexBlock;
				rep.CacheId = (options.BlockCache != null ? options.BlockCache.NewId() : 0);
				rep.FilterData = ByteArrayPointer.Null;
				rep.Filter = null;
				table = new Table(rep);
				table.ReadMeta(footer);
			}

			return s;
		}

		/// <summary>				
		/// Returns a new iterator over the table contents.
		/// The result of NewIterator() is initially invalid (caller must
		/// call one of the Seek methods on the iterator before using it).
		/// </summary>
		public Iterator NewIterator(ReadOptions options)
		{
			return TwoLevelIterator.NewTwoLevelIterator(
			 rep_.IndexBlock.NewIterator(rep_.Options.Comparator),
			 Table.BlockReader, this, options);
		}

		/// <summary>				
		/// Given a key, return an approximate byte offset in the file where
		/// the data for that key begins (or would begin if the key were
		/// present in the file).  The returned value is in terms of file
		/// bytes, and so includes effects like compression of the underlying data.
		/// E.g., the approximate offset of the last key in the table will
		/// be close to the file length.
		/// </summary>				
		public UInt64 ApproximateOffsetOf(Slice key)
		{
			Iterator index_iter =
			rep_.IndexBlock.NewIterator(rep_.Options.Comparator);
			index_iter.Seek(key);
			UInt64 result;
			if (index_iter.Valid)
			{
				BlockHandle handle = new BlockHandle();
				Slice input = index_iter.Value;
				Status s = handle.DecodeFrom(ref input);
				if (s.IsOk)
				{
					result = handle.Offset;
				}
				else
				{
					// Strange: we can't decode the block handle in the index block.
					// We'll just return the offset of the metaindex block, which is
					// close to the whole file size for this case.
					result = rep_.MetaindexHandle.Offset;
				}
			}
			else
			{
				// key is past the last key in the file.  Approximate the offset
				// by returning the offset of the metaindex block (which is
				// right near the end of the file).
				result = rep_.MetaindexHandle.Offset;
			}
			
			return result;
		}

		private static void ReleaseBlock(object arg, object h)
		{
			Cache cache = (Cache)arg;
			Cache.Handle handle = (Cache.Handle)h;
			cache.Release(handle);
		}
	}
}
