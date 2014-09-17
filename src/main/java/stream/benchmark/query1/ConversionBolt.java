package stream.benchmark.query1;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class ConversionBolt extends BaseRichBolt {
	private static final long serialVersionUID = 1L;
	OutputCollector _collector;

	public void execute(Tuple input) {
		String tuple = input.getString(0);
		String[] fields = tuple.split(",");
		double price = Double.valueOf(fields[3]);
		_collector.emit(new Values(fields[0], fields[1], fields[2], price,
				price * 1.5));
	}

	public void prepare(Map arg0, TopologyContext arg1,
			OutputCollector collector) {
		_collector = collector;
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("auction_id", "datetime", "person_id",
				"usd", "euro"));
	}

}
