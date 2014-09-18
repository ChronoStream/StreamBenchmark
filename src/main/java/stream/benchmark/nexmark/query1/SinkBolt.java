package stream.benchmark.nexmark.query1;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class SinkBolt extends BaseRichBolt {
	private static final long serialVersionUID = 1L;
	OutputCollector _collector;
	
	public void execute(Tuple input) {
		double usd=input.getDouble(3);
		double euro=input.getDouble(4);
		System.out.println("usd="+usd+", euro="+euro);
	}

	public void prepare(Map arg0, TopologyContext arg1, OutputCollector collector) {
		_collector=collector;
	}

	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		
	}

}
