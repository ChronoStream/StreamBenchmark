package stream.benchmark.nexmark.query4;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import stream.benchmark.nexmark.query4.CategoryPriceState.AuctionInfo;
import stream.benchmark.nexmark.query4.CategoryPriceState.BidInfo;
import stream.benchmark.nexmark.query4.CategoryPriceState.PriceStat;
import stream.benchmark.toolkits.MemoryReport;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class CategoryPriceCacheBolt extends BaseRichBolt {
	private static final long serialVersionUID = 1L;

	private int emitCount = 0;
	private final long slidingInterval = 10000;
	private long firstPoint = 0;
	private long secondPoint = slidingInterval;

	private OutputCollector _collector;

	Map<String, AuctionInfo> auctionMap = new HashMap<String, AuctionInfo>();
	Map<String, List<BidInfo>> bidMap = new HashMap<String, List<BidInfo>>();

	private Connection connection = null;
	private Statement statement = null;
	private PreparedStatement auctionInsertion = null;
	private PreparedStatement bidInsertion = null;

	private long measureBeginTime, measureElapsedTime;

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
			try {
				auctionInsertion.setString(1, auction_id);
				auctionInsertion.setInt(2, category);
				auctionInsertion.setLong(3, begin_time);
				auctionInsertion.setLong(4, end_time);
				auctionInsertion.addBatch();
			} catch (Exception e) {
				e.printStackTrace();
			}
			auctionMap.put(auction_id, new AuctionInfo(category, begin_time,
					end_time));
		} else if (streamname == "bid") {
			// schema: auction_id, date_time, person_id, price
			String auction_id = fields[0];
			long date_time = Long.valueOf(fields[1]);
			float price = Float.valueOf(fields[3]);
			try {
				bidInsertion.setString(1, auction_id);
				bidInsertion.setLong(2, date_time);
				bidInsertion.setFloat(3, price);
				bidInsertion.addBatch();
			} catch (Exception e) {
				e.printStackTrace();
			}
			if (!bidMap.containsKey(auction_id)) {
				bidMap.put(auction_id, new ArrayList<BidInfo>());
			}
			bidMap.get(auction_id).add(new BidInfo(date_time, price));

			if (date_time > secondPoint) {
				processQuery();
				firstPoint += slidingInterval;
				secondPoint += slidingInterval;
				emitCount += 1;
				measureElapsedTime = System.currentTimeMillis()
						- measureBeginTime;
				System.out.println("elapsed time=" + measureElapsedTime + "ms");
				measureBeginTime = System.currentTimeMillis();
			}
		}
		if (emitCount != 0 && emitCount % 2000 == 0) {
			MemoryReport.reportStatus();
			checkpoint();
		}
	}

	protected void processQuery() {
		// find maximum price for each auction.
		HashMap<String, Float> auctionMaxPrice = new HashMap<String, Float>();
		for (String auction_id : auctionMap.keySet()) {
			if (bidMap.containsKey(auction_id)) {
				List<BidInfo> tmpBidList = bidMap.get(auction_id);
				float maxPrice = -1;
				for (BidInfo tmpBid : tmpBidList) {
					if (tmpBid.date_time > firstPoint
							&& tmpBid.price > maxPrice) {
						maxPrice = tmpBid.price;
					}
				}
				if (maxPrice != -1) {
					auctionMaxPrice.put(auction_id, maxPrice);
				}
			}
		}
		// compute average price for each category.
		HashMap<Integer, PriceStat> categoryAveragePrice = new HashMap<Integer, PriceStat>();
		for (String auction_id : auctionMap.keySet()) {
			int category = auctionMap.get(auction_id).category;
			if (auctionMaxPrice.containsKey(auction_id)) {
				if (!categoryAveragePrice.containsKey(category)) {
					categoryAveragePrice.put(category, new PriceStat());
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

	private void checkpoint() {
		try {
			auctionInsertion.executeBatch();
			bidInsertion.executeBatch();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void prepare(Map arg0, TopologyContext arg1,
			OutputCollector collector) {
		_collector = collector;
		databaseInit();
		measureBeginTime = System.currentTimeMillis();
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("category", "price", "emitcount"));
	}

	protected void databaseInit() {
		try {
			connection = DriverManager.getConnection(
					"jdbc:mysql://localhost:3306/testdb", "root", "");
			statement = connection.createStatement();
			statement.executeUpdate("drop table auctiontable");
			statement.executeUpdate("drop table bidtable");

			statement
					.executeUpdate("create table auctiontable"
							+ "(auction_id varchar(20), category int, begin_time bigint, end_time bigint) "
							+ "engine=memory");
			statement
					.executeUpdate("create index auctionindex on auctiontable(auction_id)");

			statement
					.executeUpdate("create table bidtable"
							+ "(auction_id varchar(20), date_time bigint, price float) "
							+ "engine=memory");
			statement
					.executeUpdate("create index bidindex on bidtable(auction_id)");

			auctionInsertion = connection
					.prepareStatement("insert into auctiontable values(?, ?, ?, ?)");
			bidInsertion = connection
					.prepareStatement("insert into bidtable values(?, ?, ?)");

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
