package stream.benchmark.tpcc.query;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class StateMachineSink extends BaseRichBolt {

	private static final long serialVersionUID = 1L;

	public void execute(Tuple input) {
		String tuple = input.getString(0);
		System.out.println(tuple);
	}

	public void prepare(Map arg0, TopologyContext arg1, OutputCollector arg2) {
		
	}

	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		
	}

}
