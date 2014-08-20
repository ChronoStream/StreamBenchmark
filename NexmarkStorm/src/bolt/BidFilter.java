package bolt;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class BidFilter extends BaseRichBolt {
	private static final long serialVersionUID = 1L;
	OutputCollector _collector;
	long count=0;
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		_collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		//schema: auction_id, datetime, person_id, price
		String tuple=input.getString(0);
		String[] fields = tuple.split(",");
		_collector.emit(new Values(fields[0], fields[1], fields[2], fields[3]));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("auction_id", "datetime", "person_id", "price"));
	}

}
