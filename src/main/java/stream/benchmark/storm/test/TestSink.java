package stream.benchmark.storm.test;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class TestSink extends BaseRichBolt {

	int count=0;
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		
	}

	public void execute(Tuple input) {
		count+=1;
		if(count%1000==0){
			System.out.println("count="+count);
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
	}

}
