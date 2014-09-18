package stream.benchmark.nexmark.query4;

import java.util.LinkedList;
import java.util.List;

class CategoryPriceState {
	protected static class AuctionInfo {
		public AuctionInfo(int category, long begin_time, long end_time) {
			this.category = category;
			this.begin_time = begin_time;
			this.end_time = end_time;
		}

		int category;
		long begin_time;
		long end_time;
	}

	protected static class AuctionTreeInfo {
		public AuctionTreeInfo(int category, long begin_time, long end_time) {
			this.category = category;
			this.begin_time = begin_time;
			this.end_time = end_time;
			this.bidList = new LinkedList<BidInfo>();
		}

		int category;
		long begin_time;
		long end_time;
		List<BidInfo> bidList;
	}

	protected static class BidInfo {
		public BidInfo(long date_time, float price) {
			this.date_time = date_time;
			this.price = price;
		}

		long date_time;
		float price;
	}

	protected static class PriceStat {
		public PriceStat() {
			price = 0;
			count = 0;
		}

		void addPrice(float p) {
			price += p;
			count += 1;
		}

		float computeAverage() {
			return price / count;
		}

		float price;
		int count;
	}
}
