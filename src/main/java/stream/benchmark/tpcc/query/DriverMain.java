package stream.benchmark.tpcc.query;

import stream.benchmark.tpcc.spout.TpccSpout;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.TopologyBuilder;

public class DriverMain {

	public static void main(String[] args) {
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("spout", new TpccSpout(10, 1));
		BoltDeclarer bolt = builder.setBolt("bolt", new StateMachineHybridBolt());
		
		bolt.globalGrouping("spout", "item");
		bolt.globalGrouping("spout", "warehouse");
		bolt.globalGrouping("spout", "district");
		bolt.globalGrouping("spout", "customer");
		bolt.globalGrouping("spout", "stock");
		bolt.globalGrouping("spout", "order");
		bolt.globalGrouping("spout", "neworder");
		bolt.globalGrouping("spout", "orderline");
		bolt.globalGrouping("spout", "history");
		
		bolt.globalGrouping("spout", "DELIVERY");
		bolt.globalGrouping("spout", "NEW_ORDER");
		bolt.globalGrouping("spout", "ORDER_STATUS");
		bolt.globalGrouping("spout", "PAYMENT");
		bolt.globalGrouping("spout", "STOCK_LEVEL");

		bolt.globalGrouping("spout", "trigger");
		
		builder.setBolt("sink", new StateMachineSink()).globalGrouping("bolt");
		
		Config conf=new Config();
		conf.setDebug(false);
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("tpcc", conf, builder.createTopology());
	}

}
