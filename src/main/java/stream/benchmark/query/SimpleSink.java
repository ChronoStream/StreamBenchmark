package stream.benchmark.query;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class SimpleSink extends BaseRichBolt {
	private static final long serialVersionUID = 1L;
	OutputCollector _collector;
	long count=0;
	
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		_collector = collector;
	}

	public void execute(Tuple input) {
		String tuple=input.getString(0);
		System.out.println("tuple="+tuple);
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

}
