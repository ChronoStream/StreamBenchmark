package stream.benchmark.tpch.spout;

import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;

public class TpchSpout extends BaseRichSpout {
	private static final long serialVersionUID = 1L;

	SpoutOutputCollector _collector;
	
	public void nextTuple() {
		
	}

	public void open(Map arg0, TopologyContext arg1, SpoutOutputCollector collector) {
		_collector = collector;
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream("tuple", new Fields("streamname"));
	}
	
}
