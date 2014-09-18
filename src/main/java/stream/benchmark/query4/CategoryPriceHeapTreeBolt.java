package stream.benchmark.query4;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import stream.benchmark.query4.CategoryPriceState.AuctionTreeInfo;
import stream.benchmark.query4.CategoryPriceState.BidInfo;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class CategoryPriceHeapTreeBolt extends BaseRichBolt {
	private static final long serialVersionUID = 1L;

	private int emitCount = 0;
	private final long slidingInterval = 10000;
	private long firstPoint = 0;
	private long secondPoint = slidingInterval;

	private OutputCollector _collector;

	Map<String, AuctionTreeInfo> auctionTreeMap = new HashMap<String, AuctionTreeInfo>();

	public void execute(Tuple input) {
		String tuple = input.getString(0);
		String[] fields = tuple.split(",");
		String streamname = input.getSourceStreamId();
		if (streamname == "auction") {
			// schema: auction_id, seller_id, category_id, begin_time, end_time
			String auction_id = fields[0];
			int category = Integer.valueOf(fields[2]);
			long begin_time = Long.valueOf(fields[3]);
			long end_time = Long.valueOf(fields[4]);
			auctionTreeMap.put(auction_id, new AuctionTreeInfo(category,
					begin_time, end_time));
		} else if (streamname == "bid") {
			// schema: auction_id, date_time, person_id, price
			String auction_id = fields[0];
			long date_time = Long.valueOf(fields[1]);
			float price = Float.valueOf(fields[3]);
			auctionTreeMap.get(auction_id).bidList.add(new BidInfo(date_time,
					price));
			if (date_time > secondPoint) {
				processQuery();
				firstPoint += slidingInterval;
				secondPoint += slidingInterval;
				emitCount += 1;
			}
		}
	}

	protected class SimplePriceCount {
		public SimplePriceCount() {
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

	protected void processQuery() {
		// find maximum price for each auction.
		HashMap<String, Float> auctionMaxPrice = new HashMap<String, Float>();
		for (String auction_id : auctionTreeMap.keySet()) {
			List<BidInfo> tmpBidList = auctionTreeMap.get(auction_id).bidList;
			float maxPrice = -1;
			for (BidInfo tmpBid : tmpBidList) {
				if (tmpBid.date_time > firstPoint && tmpBid.price > maxPrice) {
					maxPrice = tmpBid.price;
				}
			}
			if (maxPrice != -1) {
				auctionMaxPrice.put(auction_id, maxPrice);
			}
		}
		// compute average price for each category.
		HashMap<Integer, SimplePriceCount> categoryAveragePrice = new HashMap<Integer, SimplePriceCount>();
		for (String auction_id : auctionTreeMap.keySet()) {
			int category = auctionTreeMap.get(auction_id).category;
			if (auctionMaxPrice.containsKey(auction_id)) {
				if (!categoryAveragePrice.containsKey(category)) {
					categoryAveragePrice.put(category, new SimplePriceCount());
				}
				categoryAveragePrice.get(category).addPrice(
						auctionMaxPrice.get(auction_id));
			}
		}
		// emit computation result.
		for (int category : categoryAveragePrice.keySet()) {
			_collector.emit(new Values(category, categoryAveragePrice.get(
					category).computeAverage(), emitCount));
		}
	}

	public void prepare(Map arg0, TopologyContext arg1,
			OutputCollector collector) {
		_collector = collector;
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("category", "price", "emitcount"));
	}

}
