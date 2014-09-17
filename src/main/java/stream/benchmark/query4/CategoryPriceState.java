package stream.benchmark.query4;

class CategoryPriceState {
	protected static class AuctionInfo {
		public AuctionInfo(int category, long begin_time,
				long end_time) {
			this.category = category;
			this.begin_time = begin_time;
			this.end_time = end_time;
		}

		int category;
		long begin_time;
		long end_time;
	}

	protected static class BidInfo {
		public BidInfo(long date_time, float price) {
			this.date_time = date_time;
			this.price = price;
		}

		long date_time;
		float price;
	}
}
