package stream.benchmark.storm.test;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class TestBolt extends BaseRichBolt {

	OutputCollector _collector;
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		_collector=collector;
	}

	public void execute(Tuple input) {
		String str=input.getString(0);
		_collector.emit(new Values("mystring"));
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("tuple"));
	}

}
