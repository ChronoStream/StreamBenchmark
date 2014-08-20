package bolt;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class SimpleSink extends BaseRichBolt {
	
	OutputCollector _collector;
	long count=0;
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		_collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		Integer value=input.getInteger(0);
		String itemid=input.getString(1);
		System.out.println("max value="+value+", item id="+itemid);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

}
