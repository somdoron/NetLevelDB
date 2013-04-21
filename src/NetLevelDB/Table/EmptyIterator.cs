namespace NetLevelDB.Table
{
	class EmptyIterator : Iterator
	{
		private Status m_status;

		public EmptyIterator(Status status)
		{
			m_status = status;
		}

		public override bool Valid
		{
			get { return false; }
		}

		public override void SeekToFirst()
		{
			
		}

		public override void SeekToLast()
		{

		}

		public override void Seek(Slice target)
		{

		}

		public override void Next()
		{

		}

		public override void Prev()
		{
			
		}

		public override Slice Key
		{
			get { return new Slice(); }
		}

		public override Slice Value
		{
			get { return  new Slice();}
		}

		public override Status Status
		{
			get { return m_status; }			
		}
	}
}
