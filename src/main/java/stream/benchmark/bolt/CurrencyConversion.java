package stream.benchmark.bolt;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class CurrencyConversion extends BaseRichBolt {
	private static final long serialVersionUID = 1L;
	OutputCollector _collector;
	
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		_collector=collector;
	}

	public void execute(Tuple input) {
		String auction_id=input.getString(0);
		String datetime=input.getString(1);
		String person_id=input.getString(2);
		String price=input.getString(3);
		String newPrice=String.valueOf((int)(Integer.valueOf(price)*1.2));
		_collector.emit("bid", new Values(auction_id, datetime, person_id, newPrice));
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream("bid", new Fields("auction_id", "datetime", "person_id", "price"));
	}

}
