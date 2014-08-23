package stream.benchmark.spout;

import java.util.Map;
import java.util.Random;

import stream.benchmark.relation.OpenAuctions;
import stream.benchmark.relation.PersonGen;
import stream.benchmark.relation.Persons;
import stream.benchmark.relation.SimpleCalendar;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class NexmarkSpout extends BaseRichSpout {
	private static final long serialVersionUID = 1L;

	SpoutOutputCollector _collector;

	// generate bids, items and persons
	private Random rnd = new Random();
	private SimpleCalendar cal = new SimpleCalendar();
	// for managing person ids
	private Persons persons = new Persons();
	// for managing open auctions
	private OpenAuctions openAuctions = new OpenAuctions();
	// for generating values for person
	private PersonGen p = new PersonGen();

	boolean isFirst = true;

	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		_collector = collector;
	}

	public void nextTuple() {
		if (isFirst == true) {
			// populate the stream. generate 50 persons and 50 auctions.
			for (int i = 0; i < 50; i++) {
				String str = generatePerson();
				_collector.emit("person", new Values(str));
			}
			for (int i = 0; i < 50; i++) {
				String str = generateAuction();
				_collector.emit("auction", new Values(str));
			}
			isFirst = false;
		} else {
			// now go into a loop generating bids and persons and so on
			// generating a person approximately 10th time will give is 10
			// items/person since we generate on average one bid per loop
			if (rnd.nextInt(10) == 0) {
				String str = generatePerson();
				_collector.emit("person", new Values(str));
			}
			// want on average 1 item and 10 bids
			int numItems = rnd.nextInt(3); // should average 1
			for (int i = 0; i < numItems; ++i) {
				String str = generateAuction();
				_collector.emit("auction", new Values(str));
			}
			int numBids = rnd.nextInt(21) + 1; // should average 10
			for (int i = 0; i < numBids; ++i) {
				String str = generateBid();
				_collector.emit("bid", new Values(str));
			}
		}

	}

	String generatePerson() {
		// schema: person_id, street_name, email, city, state, country
		StringBuilder myb = new StringBuilder();
		cal.incrementTime();
		p.generateValues(); // person object is reusable now
		myb.append(persons.getNewId());
		myb.append(",");
		myb.append(p.m_stName);
		myb.append(",");
		myb.append(p.m_stEmail);
		myb.append(",");
		myb.append(p.m_stCity);
		myb.append(",");
		myb.append(p.m_stProvince);
		myb.append(",");
		myb.append(p.m_stCountry);
		return myb.toString();
	}

	String generateAuction() {
		// schema: auction_id, seller_id, category_id, begin_time, end_time
		StringBuilder myb = new StringBuilder();
		cal.incrementTime();
		// at this point we are not generating items, we are generating
		// only open auctions, id for open_auction is same as id of item
		// up for auction
		long auctionId = openAuctions.getNewId();
		myb.append(auctionId);
		myb.append(",");
		// seller
		myb.append(persons.getExistingId());
		myb.append(",");
		// KT - add category id XMark items can be in 1-10 categories
		// we allow an item to be in one category
		int catid = rnd.nextInt(1000);
		myb.append(catid);
		myb.append(",");
		// interval
		long currentTime = cal.getTimeInSecs();
		myb.append(currentTime);
		myb.append(",");
		myb.append(currentTime + 60 * 60 + rnd.nextInt(24 * 60 * 60 * 2));
		return myb.toString();
	}

	String generateBid() {
		// schema: auction_id, datetime, person_id, price
		StringBuilder myb = new StringBuilder();
		cal.incrementTime();
		long itemId = openAuctions.getExistingId();
		myb.append(itemId);
		myb.append(",");
		myb.append(cal.getTimeInSecs());// here, datetime is in second;
		myb.append(",");
		myb.append(persons.getExistingId());
		myb.append(",");
		myb.append(openAuctions.getPrice());
		return myb.toString();
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream("person", new Fields("tuple"));
		declarer.declareStream("auction", new Fields("tuple"));
		declarer.declareStream("bid", new Fields("tuple"));
	}

}
