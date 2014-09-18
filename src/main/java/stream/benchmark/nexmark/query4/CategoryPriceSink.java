package stream.benchmark.nexmark.query4;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class CategoryPriceSink extends BaseRichBolt {
	private static final long serialVersionUID = 1L;
	int lastEmitCount=0;
	int count=0;
	
	public void execute(Tuple input) {
		int category = input.getInteger(0);
		float price = input.getFloat(1);
		int emitCount = input.getInteger(2);
//		System.out.println("category=" + category + ", price=" + price
//				+ ", emitCount=" + emitCount);
		if(lastEmitCount==emitCount){
			count+=1;
		}else{
			System.out.println("emitCount="+emitCount+", number="+count);
			lastEmitCount=emitCount;
			count=0;
		}
	}

	public void prepare(Map arg0, TopologyContext arg1,
			OutputCollector collector) {
	}

	public void declareOutputFields(OutputFieldsDeclarer arg0) {

	}

}
