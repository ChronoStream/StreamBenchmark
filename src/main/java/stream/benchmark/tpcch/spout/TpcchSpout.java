package stream.benchmark.tpcch.spout;

import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;

public class TpcchSpout extends BaseRichSpout {
	private static final long serialVersionUID = 1L;

	SpoutOutputCollector _collector;

	boolean _isFirst = true;
	int _warehouses = 4;
	int _scalefactor = 1;

	public TpcchSpout() {
	}

	public TpcchSpout(int warehouses, int scalefactor) {
		_warehouses = warehouses;
		_scalefactor = scalefactor;
	}

	public void nextTuple() {
		if (_isFirst == true) {
			// populate state here
		} else {
			// execute here
		}
	}

	public void open(Map arg0, TopologyContext arg1,
			SpoutOutputCollector collector) {
		_collector = collector;
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream("item", new Fields("tuple"));
		declarer.declareStream("warehouse", new Fields("tuple"));
		declarer.declareStream("district", new Fields("tuple"));
		declarer.declareStream("customer", new Fields("tuple"));
		declarer.declareStream("stock", new Fields("tuple"));
		declarer.declareStream("orders", new Fields("tuple"));
		declarer.declareStream("new_order", new Fields("tuple"));
		declarer.declareStream("order_line", new Fields("tuple"));
		declarer.declareStream("history", new Fields("tuple"));
		declarer.declareStream("trigger", new Fields("tuple"));
	}

}
