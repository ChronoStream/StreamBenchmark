package stream.benchmark.query6;

public class SellingPriceState {
	protected static class AuctionInfo {
		public AuctionInfo(String seller_id, long begin_time, long end_time) {
			this.seller_id = seller_id;
			this.begin_time = begin_time;
			this.end_time = end_time;
		}
		String seller_id;
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
