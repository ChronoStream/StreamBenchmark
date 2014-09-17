package stream.benchmark.query5;

import stream.benchmark.spout.NexmarkSpout;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.TopologyBuilder;

public class HotItemsDriverMain {

	public static void main(String[] args) throws Exception {
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("spout", new NexmarkSpout());
		BoltDeclarer categoryPrice=builder.setBolt("query", new HotItemsHeapBolt());
		categoryPrice.globalGrouping("spout", "auction");
		categoryPrice.globalGrouping("spout", "bid");
		
		builder.setBolt("sink", new HotItemsSink()).globalGrouping("query");
		
		Config conf = new Config();
		conf.setDebug(false);
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("nexmark", conf, builder.createTopology());
	}
}

