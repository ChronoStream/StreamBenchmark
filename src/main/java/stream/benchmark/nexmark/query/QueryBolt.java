package stream.benchmark.nexmark.query;

import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.TreeMap;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class QueryBolt extends BaseRichBolt {

	private static final long serialVersionUID = 1L;

	protected class PersonInfo {
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

	protected class AuctionInfo {
		public AuctionInfo(String seller, long begin_time, long end_time) {
			this.seller = seller;
			this.begin_time = begin_time;
			this.end_time = end_time;
		}
		String seller;
		long begin_time;
		long end_time;
	}

	protected class BuyerPrice {
		public BuyerPrice(String buyer_id, int price) {
			this.buyer_id = buyer_id;
			this.price = price;
		}
		public String buyer_id;
		public int price;
	}
	
	protected class ItemPrice {
		public ItemPrice(String item_id, int price){
			this.item_id = item_id;
			this.price = price;
		}
		public String item_id;
		public int price;
	}
	
	public class ValueComparator implements Comparator<String> {
	    Map<String, Integer> base;
	    public ValueComparator(Map<String, Integer> base) {
	        this.base = base;
	    }

	    // Note: this comparator imposes orderings that are inconsistent with equals.    
	    public int compare(String a, String b) {
	        if (base.get(a) >= base.get(b)) {
	            return 1;
	        } else {
	            return -1;
	        } // returning 0 would merge keys
	    }
	}

	OutputCollector _collector;
	int tupleCount=0;

	// record person information, infrequently updated.
	Map<String, PersonInfo> personMap = new HashMap<String, PersonInfo>();
	// record auction information, infrequently updated.
	Map<String, AuctionInfo> auctionMap = new HashMap<String, AuctionInfo>();
	Map<String, LinkedList<BuyerPrice>> itemPrices = new HashMap<String, LinkedList<BuyerPrice>>();
	Map<String, LinkedList<ItemPrice>> buyerPrices = new HashMap<String, LinkedList<ItemPrice>>();

	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		_collector = collector;
	}
	
	public void compute(){
		HashMap<String, Integer> itemAvgPrice=new HashMap<String, Integer>();
		//calculate average price of each item
		for(String item : itemPrices.keySet()){
			int average=0;
			for(BuyerPrice priceinfo : itemPrices.get(item)){
				average+=priceinfo.price;
			}
			average=average/itemPrices.get(item).size();
			itemAvgPrice.put(item, average);
		}
		ValueComparator itemvc=new ValueComparator(itemAvgPrice);
		TreeMap<String, Integer> sortedItemAvgPrice=new TreeMap<String, Integer>(itemvc);
		sortedItemAvgPrice.putAll(itemAvgPrice);
		int itemCount=0;
		HashMap<String, Integer> topkItemAvgPrice=new HashMap<String, Integer>();
		for (Map.Entry<String, Integer> entry : sortedItemAvgPrice.entrySet()) {
			Integer value = entry.getValue();
			String key = entry.getKey();
			topkItemAvgPrice.put(key, value);
			++itemCount;
			if(itemCount>100){
				return;
			}
		}
		LinkedList<String> bestSellers=new LinkedList<String>();
		for(String itemId : topkItemAvgPrice.keySet()){
			for(String auctionId : auctionMap.keySet()){
				if(itemId==auctionId){
					bestSellers.add(auctionMap.get(auctionId).seller);
				}
			}
		}
		HashMap<String, Integer> buyerAvgPrice=new HashMap<String, Integer>();
		for(String buyer : buyerPrices.keySet()){
			int average=0;
			for(ItemPrice priceinfo : buyerPrices.get(buyer)){
				average+=priceinfo.price;
			}
			average=average/buyerPrices.get(buyer).size();
			buyerAvgPrice.put(buyer, average);
		}
		ValueComparator buyervc=new ValueComparator(buyerAvgPrice);
		TreeMap<String, Integer> sortedBuyerAvgPrice=new TreeMap<String, Integer>(buyervc);
		sortedBuyerAvgPrice.putAll(buyerAvgPrice);
		int buyerCount=0;
		HashMap<String, Integer>topkBuyerAvgPrice=new HashMap<String, Integer>();
		for (Map.Entry<String, Integer> entry : sortedBuyerAvgPrice.entrySet()) {
			Integer value = entry.getValue();
			String key = entry.getKey();
			topkBuyerAvgPrice.put(key, value);
			++buyerCount;
			if(buyerCount>100){
				return;
			}
		}
		for(String seller : bestSellers){
			for(String buyer : topkBuyerAvgPrice.keySet()){
				if(seller==buyer){
					_collector.emit(new Values(seller));
				}
			}
		}
	}
	
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
				BuyerPrice priceInfo = new BuyerPrice(person_id, price);
				if(!itemPrices.containsKey(auction_id)){
					itemPrices.put(auction_id, new LinkedList<BuyerPrice>());
				}
				itemPrices.get(auction_id).add(priceInfo);
			}
			++tupleCount;
			if(tupleCount%1000==0){
				compute();
			}
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("person"));
	}

}
