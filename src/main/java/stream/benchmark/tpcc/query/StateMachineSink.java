package stream.benchmark.tpcc.query;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class StateMachineSink extends BaseRichBolt {

	private static final long serialVersionUID = 1L;

	private long _measureBeginTime, _measureElapsedTime;
	private int _count = 0;

	public void execute(Tuple input) {
		String tuple = input.getString(0);
		System.out.println(tuple);
		++_count;
		if (_count % 1000 == 0) {
			_measureElapsedTime = System.currentTimeMillis()
					- _measureBeginTime;
			System.out.println("elapsed time=" + _measureElapsedTime + "ms");
			_measureBeginTime = System.currentTimeMillis();
			_count = 0;
		}
	}

	public void prepare(Map arg0, TopologyContext arg1, OutputCollector arg2) {
		_measureBeginTime = System.currentTimeMillis();
	}

	public void declareOutputFields(OutputFieldsDeclarer arg0) {

	}

}
