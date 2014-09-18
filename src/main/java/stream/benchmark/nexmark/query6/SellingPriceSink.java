package stream.benchmark.nexmark.query6;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class SellingPriceSink extends BaseRichBolt {
	private static final long serialVersionUID = 1L;
	int lastEmitCount = 0;
	int count = 0;

	public void execute(Tuple input) {
		String auction_id = input.getString(0);
		String seller_id = input.getString(1);
		int bidCount = input.getInteger(2);
		float price = input.getFloat(3);
		int emitCount = input.getInteger(4);
		System.out.println("auction_id=" + auction_id + ", seller_id="
				+ seller_id + ", bidCount=" + bidCount + ", price=" + price
				+ ", emitCount=" + emitCount);
		if (lastEmitCount == emitCount) {
			count += 1;
		} else {
			System.out.println("emitCount=" + emitCount + ", number=" + count);
			lastEmitCount = emitCount;
			count = 0;
		}
	}

	public void prepare(Map arg0, TopologyContext arg1,
			OutputCollector collector) {
	}

	public void declareOutputFields(OutputFieldsDeclarer arg0) {

	}

}
