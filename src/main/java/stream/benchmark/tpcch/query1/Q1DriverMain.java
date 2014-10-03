package stream.benchmark.tpcch.query1;

import stream.benchmark.tpcch.spout.TpcchSpout;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.TopologyBuilder;

public class Q1DriverMain {

	public static void main(String[] args) {
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("spout", new TpcchSpout());
		BoltDeclarer bolt = builder.setBolt("bolt", new Q1OptBolt2());
		
		bolt.globalGrouping("spout", "item");
		bolt.globalGrouping("spout", "district");
		bolt.globalGrouping("spout", "neworder");
		bolt.globalGrouping("spout", "orderline");
		
		bolt.globalGrouping("spout", "DELIVERY");
		bolt.globalGrouping("spout", "NEW_ORDER");

		builder.setBolt("sink", new Q1Sink()).globalGrouping("bolt");
		
		Config conf=new Config();
		conf.setDebug(false);
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("tpcch", conf, builder.createTopology());
	}

}
