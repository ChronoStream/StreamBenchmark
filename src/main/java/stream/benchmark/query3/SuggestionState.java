package stream.benchmark.query3;

import java.util.LinkedList;
import java.util.List;

public class SuggestionState {

	protected static class PersonInfo {
		public PersonInfo(String city, String state, String country) {
			this.city = city;
			this.state = state;
			this.country = country;
		}

		String city;
		String state;
		String country;
	}

	protected static class PersonTreeInfo {
		public PersonTreeInfo(String city, String state, String country) {
			this.city = city;
			this.state = state;
			this.country = country;
			this.auctionList = new LinkedList<AuctionInfo>();
		}

		String city;
		String state;
		String country;
		List<AuctionInfo> auctionList;
	}

	protected static class AuctionInfo {
		public AuctionInfo(String auction_id, int category, long begin_time) {
			this.auction_id = auction_id;
			this.category = category;
			this.begin_time = begin_time;
		}

		String auction_id;
		int category;
		long begin_time;
	}

}
