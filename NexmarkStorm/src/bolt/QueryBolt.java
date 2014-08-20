package bolt;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class QueryBolt extends BaseRichBolt {

	public class PersonInfo {
		public PersonInfo(String email, String city, String state,
				String country) {
			this.email = email;
			this.city = city;
			this.state = state;
			this.country = country;
		}

		String email;
		String city;
		String state;
		String country;
	}

	public class AuctionInfo {
		public AuctionInfo(String seller, long begin_time, long end_time) {
			this.seller = seller;
			this.begin_time = begin_time;
			this.end_time = end_time;
		}

		String seller;
		long begin_time;
		long end_time;
	}

	public class PriceInfo {
		public PriceInfo(String buyer_id, int price) {
			this.buyer_id = buyer_id;
			this.price = price;
		}

		String buyer_id;
		int price;
	}

	OutputCollector _collector;
	int tupleCount=0;

	// record person information, infrequently updated.
	HashMap<String, PersonInfo> personMap = new HashMap<String, PersonInfo>();
	// record auction information, infrequently updated.
	HashMap<String, AuctionInfo> auctionMap = new HashMap<String, AuctionInfo>();
	HashMap<String, LinkedList<PriceInfo>> itemPrices = new HashMap<String, LinkedList<PriceInfo>>();

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		_collector = collector;
	}
	
	public void compute(){
		HashMap<String, Integer> itemAvgPrice=new HashMap<String, Integer>();
		for(String item : itemPrices.keySet()){
			int average=0;
			for(PriceInfo priceinfo : itemPrices.get(item)){
				average+=priceinfo.price;
			}
			average=average/itemPrices.get(item).size();
			itemAvgPrice.put(item, average);
		}
		int maxValue=0;
		String maxItemId="";
		for(String itemId : itemAvgPrice.keySet()){
			if(itemAvgPrice.get(itemId)>maxValue){
				maxValue=itemAvgPrice.get(itemId);
				maxItemId=itemId;
			}
		}
		_collector.emit(new Values(maxValue, maxItemId));
	}
	
	@Override
	public void execute(Tuple input) {
		String streamname = input.getSourceStreamId();
		if (streamname == "person") {
			// schema: person_id, street_name, email, city, state, country
			String person_id = input.getString(0);
			String email = input.getString(2);
			String city = input.getString(3);
			String state = input.getString(4);
			String country = input.getString(5);
			PersonInfo personInfo = new PersonInfo(email, city, state, country);
			personMap.put(person_id, personInfo);

		} else if (streamname == "auction") {
			// schema: auction_id, seller_id, category_id, begin_time, end_time
			String auction_id = input.getString(0);
			String seller_id = input.getString(1);
			Long begin_time = Long.valueOf(input.getString(3));
			Long end_time = Long.valueOf(input.getString(4));
			AuctionInfo auctionInfo = new AuctionInfo(seller_id, begin_time,
					end_time);
			auctionMap.put(auction_id, auctionInfo);

		} else if (streamname == "bid") {
			// schema: auction_id, datetime, person_id, price
			String auction_id = input.getString(0);
			Long datetime = Long.valueOf(input.getString(1));
			String person_id = input.getString(2);
			int price = Integer.valueOf(input.getString(3));
			// check whether applicable.
			if (auctionMap.containsKey(auction_id)
					&& datetime > auctionMap.get(auction_id).begin_time
					&& datetime < auctionMap.get(auction_id).end_time) {
				PriceInfo priceInfo = new PriceInfo(person_id, price);
				if(!itemPrices.containsKey(auction_id)){
					itemPrices.put(auction_id, new LinkedList<PriceInfo>());
				}
				itemPrices.get(auction_id).add(priceInfo);
			}
			++tupleCount;
			if(tupleCount%1000==0){
				compute();
			}
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("maxvalue", "itemid"));
	}

}
