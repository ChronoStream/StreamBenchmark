package stream.benchmark.tpch.spout;

import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;

public class TpchSpout extends BaseRichSpout {

	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		
	}

	public void nextTuple() {
		
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
	}

}
