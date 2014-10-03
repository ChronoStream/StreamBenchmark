package stream.benchmark.tpcch.query3;

import stream.benchmark.tpcch.spout.TpcchSpout;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.TopologyBuilder;

public class Q3DriverMain {

	public static void main(String[] args) {
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("spout", new TpcchSpout());
		BoltDeclarer bolt = builder.setBolt("bolt", new Q3HeapBolt());
		
		bolt.globalGrouping("spout", "item");
		bolt.globalGrouping("spout", "district");
		bolt.globalGrouping("spout", "customer");
		bolt.globalGrouping("spout", "order");
		bolt.globalGrouping("spout", "neworder");
		bolt.globalGrouping("spout", "orderline");
		
		bolt.globalGrouping("spout", "DELIVERY");
		bolt.globalGrouping("spout", "NEW_ORDER");

		builder.setBolt("sink", new Q3Sink()).globalGrouping("bolt");
		
		Config conf=new Config();
		conf.setDebug(false);
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("tpcch", conf, builder.createTopology());
	}

}
