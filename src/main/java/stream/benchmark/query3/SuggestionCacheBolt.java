package stream.benchmark.query3;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import stream.benchmark.query3.SuggestionState.AuctionInfo;
import stream.benchmark.query3.SuggestionState.PersonInfo;
import stream.benchmark.toolkits.MemoryReport;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class SuggestionCacheBolt extends BaseRichBolt {
	private static final long serialVersionUID = 1L;

	private int emitCount = 0;
	private final long slidingInterval = 100000;
	private long firstPoint = 0;
	private long secondPoint = slidingInterval;

	OutputCollector _collector;
	// record person information, infrequently updated.
	Map<String, PersonInfo> personMap = new HashMap<String, PersonInfo>();
	// record auction information, infrequently updated.
	Map<String, List<AuctionInfo>> auctionMap = new HashMap<String, List<AuctionInfo>>();

	private Connection connection = null;
	private Statement statement = null;
	private PreparedStatement personInsertion = null;
	private PreparedStatement auctionInsertion = null;

	private long measureBeginTime, measureElapsedTime;

	public void execute(Tuple input) {
		String tuple = input.getString(0);
		String[] fields = tuple.split(",");
		String streamname = input.getSourceStreamId();
		if (streamname == "auction") {
			// schema: auction_id, seller_id, category_id, begin_time, end_time
			String auction_id = fields[0];
			String seller_id = fields[1];
			int category = Integer.valueOf(fields[2]);
			long begin_time = Long.valueOf(fields[3]);
			try {
				auctionInsertion.setString(1, seller_id);
				auctionInsertion.setString(2, auction_id);
				auctionInsertion.setInt(3, category);
				auctionInsertion.setLong(4, begin_time);
				auctionInsertion.addBatch();
			} catch (Exception e) {
				e.printStackTrace();
			}
			AuctionInfo auctionInfo = new AuctionInfo(auction_id, category,
					begin_time);
			if (!auctionMap.containsKey(seller_id)) {
				auctionMap.put(seller_id, new LinkedList<AuctionInfo>());
			}
			auctionMap.get(seller_id).add(auctionInfo);

			if (begin_time > secondPoint) {
				processQuery();
				firstPoint += slidingInterval;
				secondPoint += slidingInterval;
				emitCount += 1;
				measureElapsedTime = System.currentTimeMillis()
						- measureBeginTime;
				System.out.println("elapsed time=" + measureElapsedTime + "ms");
				measureBeginTime = System.currentTimeMillis();
			}

		} else if (streamname == "person") {
			// schema: person_id, street_name, email, city, state, country
			String person_id = fields[0];
			String city = fields[3];
			String state = fields[4];
			String country = fields[5];
			try {
				personInsertion.setString(1, person_id);
				personInsertion.setString(2, city);
				personInsertion.setString(3, state);
				personInsertion.setString(4, country);
				personInsertion.addBatch();
			} catch (Exception e) {
				e.printStackTrace();
			}
			PersonInfo personInfo = new PersonInfo(city, state, country);
			personMap.put(person_id, personInfo);
		}
		if (emitCount != 0 && emitCount % 2000 == 0) {
			System.out.println("emit count=" + emitCount);
			MemoryReport.reportStatus();
			// checkpoint here!
			try {
				personInsertion.executeBatch();
				auctionInsertion.executeBatch();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	protected void processQuery() {
		for (String person_id : personMap.keySet()) {
			if (personMap.get(person_id).country.equals("United States")
					&& auctionMap.containsKey(person_id)) {
				List<AuctionInfo> tmpAuctionList = auctionMap.get(person_id);
				PersonInfo tmpPerson = personMap.get(person_id);
				for (AuctionInfo tmpAuction : tmpAuctionList) {
					if (tmpAuction.begin_time > firstPoint
							&& tmpAuction.category % 10 == 0) {
						_collector.emit(new Values(person_id,
								tmpAuction.auction_id, tmpPerson.city,
								tmpPerson.state, tmpPerson.country,
								tmpAuction.category, emitCount));
					}
				}
			}
		}
	}

	public void prepare(Map arg0, TopologyContext arg1,
			OutputCollector collector) {
		_collector = collector;
		databaseInit();
		measureBeginTime = System.currentTimeMillis();
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("person_id", "auction_id", "city", "state",
				"country", "category", "emitcount"));
	}

	private void databaseInit() {
		try {
			connection = DriverManager.getConnection(
					"jdbc:mysql://localhost:3306/testdb", "root", "");
			statement = connection.createStatement();
			statement.executeUpdate("drop table persontable");
			statement.executeUpdate("drop table auctiontable");
			statement
					.executeUpdate("create table persontable"
							+ "(person_id varchar(20), city varchar(20), province varchar(20), country varchar(30)) "
							+ "engine=memory");
			statement
					.executeUpdate("create index personindex on persontable(person_id)");
			statement
					.executeUpdate("create table auctiontable"
							+ "(seller varchar(20), auction_id varchar(20), category int, begin_time bigint) "
							+ "engine=memory");
			statement
					.executeUpdate("create index auctionindex on auctiontable(seller)");

			personInsertion = connection
					.prepareStatement("insert into persontable values(?, ?, ?, ?)");
			auctionInsertion = connection
					.prepareStatement("insert into auctiontable values(?, ?, ?, ?)");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
