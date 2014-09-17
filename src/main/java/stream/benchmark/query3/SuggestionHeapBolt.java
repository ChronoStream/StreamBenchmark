package stream.benchmark.query3;

import java.util.HashMap;
import java.util.Map;

import stream.benchmark.query3.SuggestionState.AuctionInfo;
import stream.benchmark.query3.SuggestionState.PersonInfo;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class SuggestionHeapBolt extends BaseRichBolt {
	private static final long serialVersionUID = 1L;

	private int emitCount = 0;
	private final long slidingInterval = 1000;
	private long firstPoint = 0;
	private long secondPoint = slidingInterval;
	
	OutputCollector _collector;
	// record person information, infrequently updated.
	Map<String, PersonInfo> personMap = new HashMap<String, PersonInfo>();
	// record auction information, infrequently updated.
	Map<String, AuctionInfo> auctionMap = new HashMap<String, AuctionInfo>();

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
			AuctionInfo auctionInfo = new AuctionInfo(auction_id, category,
					begin_time);
			auctionMap.put(seller_id, auctionInfo);

			if (begin_time > secondPoint) {
				processQuery();
				firstPoint += slidingInterval;
				secondPoint += slidingInterval;
				emitCount += 1;
			}

		} else if (streamname == "person") {
			// schema: person_id, street_name, email, city, state, country
			String person_id = fields[0];
			String city = fields[3];
			String state = fields[4];
			String country = fields[5];
			PersonInfo personInfo = new PersonInfo(city, state, country);
			personMap.put(person_id, personInfo);
		}
	}
	
	protected void processQuery(){
		for (String person_id : personMap.keySet()) {
			if (personMap.get(person_id).country
					.equals("United States")
					&& auctionMap.containsKey(person_id)) {
				AuctionInfo tmpAuction = auctionMap.get(person_id);
				PersonInfo tmpPerson = personMap.get(person_id);
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

	public void prepare(Map arg0, TopologyContext arg1,
			OutputCollector collector) {
		_collector = collector;
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("person_id", "auction_id", "city", "state",
				"country", "category", "emitcount"));
	}

}
