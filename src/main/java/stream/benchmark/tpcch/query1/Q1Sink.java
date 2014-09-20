package stream.benchmark.tpcch.query1;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class Q1Sink extends BaseRichBolt {

	private static final long serialVersionUID = 1L;

	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		
	}

	public void execute(Tuple input) {
		
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
	}

}
