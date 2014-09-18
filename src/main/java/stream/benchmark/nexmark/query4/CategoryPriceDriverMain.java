package stream.benchmark.nexmark.query4;

import stream.benchmark.nexmark.spout.NexmarkSpout;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.TopologyBuilder;

public class CategoryPriceDriverMain {

	public static void main(String[] args) throws Exception {
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("spout", new NexmarkSpout());
		BoltDeclarer categoryPrice=builder.setBolt("query", new CategoryPriceCacheBolt());
		categoryPrice.globalGrouping("spout", "auction");
		categoryPrice.globalGrouping("spout", "bid");
		
		builder.setBolt("sink", new CategoryPriceSink()).globalGrouping("query");
		
		Config conf = new Config();
		conf.setDebug(false);
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("nexmark", conf, builder.createTopology());
	}
}

