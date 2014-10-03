package stream.benchmark.tpcch.query2;

import stream.benchmark.tpcch.spout.TpcchSpout;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.TopologyBuilder;

public class Q2DriverMain {

	public static void main(String[] args) {
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("spout", new TpcchSpout(5, 0.005));
		BoltDeclarer bolt = builder.setBolt("bolt", new Q2OptBolt1());

		bolt.globalGrouping("spout", "item");
		bolt.globalGrouping("spout", "stock");
		bolt.globalGrouping("spout", "supplier");
		bolt.globalGrouping("spout", "region");
		bolt.globalGrouping("spout", "nation");
		
		bolt.globalGrouping("spout", "NEW_ORDER");

		builder.setBolt("sink", new Q2Sink()).globalGrouping("bolt");
		
		Config conf=new Config();
		conf.setDebug(false);
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("tpcch", conf, builder.createTopology());
	}

}
