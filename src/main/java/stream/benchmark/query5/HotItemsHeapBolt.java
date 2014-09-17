package stream.benchmark.query5;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import stream.benchmark.query5.HotItemsState.AuctionInfo;
import stream.benchmark.query5.HotItemsState.BidInfo;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class HotItemsHeapBolt extends BaseRichBolt {
	private static final long serialVersionUID = 1L;

	private int emitCount = 0;
	private final long slidingInterval = 10000;
	private long firstPoint = 0;
	private long secondPoint = slidingInterval;
	
	private OutputCollector _collector;
	
	Map<String, AuctionInfo> auctionMap = new HashMap<String, AuctionInfo>();
	Map<String, List<BidInfo>> bidMap = new HashMap<String, List<BidInfo>>();
	
	public void execute(Tuple input) {
		String tuple = input.getString(0);
		String[] fields = tuple.split(",");
		String streamname = input.getSourceStreamId();
		if(streamname == "auction"){
			// schema: auction_id, seller_id, category_id, begin_time, end_time
			String auction_id = fields[0];
			String seller_id = fields[1];
			long begin_time = Long.valueOf(fields[3]);
			long end_time = Long.valueOf(fields[4]);
			auctionMap.put(auction_id, new AuctionInfo(seller_id, begin_time, end_time));
		}else if(streamname == "bid"){
			// schema: auction_id, date_time, person_id, price
			String auction_id = fields[0];
			long date_time = Long.valueOf(fields[1]);
			float price = Float.valueOf(fields[3]);
			if(!bidMap.containsKey(auction_id)){
				bidMap.put(auction_id, new ArrayList<BidInfo>());
			}
			bidMap.get(auction_id).add(new BidInfo(date_time, price));
			if (date_time > secondPoint) {
				processQuery();
				firstPoint += slidingInterval;
				secondPoint += slidingInterval;
				emitCount += 1;
			}
		}
	}

	protected void processQuery(){
		int hotCount=-1;
		String hotId = "";
		for (String auction_id : bidMap.keySet()){
			int count = 0;
			List<BidInfo> tmpBidList = bidMap.get(auction_id);
			for(BidInfo tmpBid : tmpBidList){
				if (tmpBid.date_time > firstPoint){
					count += 1;
				}
			}
			if (count > hotCount){
				hotCount = count;
				hotId = auction_id;
			}
		}
		if (hotCount != -1){
			List<BidInfo> tmpBidList = bidMap.get(hotId);
			float maxPrice = -1;
			for (BidInfo tmpBid : tmpBidList){
				if(tmpBid.date_time > firstPoint && tmpBid.price > maxPrice){
					maxPrice = tmpBid.price;
				}
			}
			String seller_id = auctionMap.get(hotId).seller_id;
			_collector.emit(new Values(hotId, seller_id, hotCount, maxPrice, emitCount));
		}
	}
	
	public void prepare(Map arg0, TopologyContext arg1, OutputCollector collector) {
		_collector = collector;
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("auction_id", "seller_id", "bidcount", "price", "emitcount"));
	}

}
