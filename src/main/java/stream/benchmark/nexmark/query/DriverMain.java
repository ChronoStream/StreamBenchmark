package stream.benchmark.nexmark.query;

import stream.benchmark.nexmark.spout.NexmarkSpout;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class DriverMain {

	public static void main(String[] args) throws Exception {
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("spout", new NexmarkSpout());
		builder.setBolt("personfilter", new PersonFilter()).globalGrouping("spout", "person");
		builder.setBolt("auctionfilter", new AuctionFilter()).globalGrouping("spout", "auction");
		builder.setBolt("bidfilter", new BidFilter()).globalGrouping("spout", "bid");
		builder.setBolt("currency", new CurrencyConversion()).globalGrouping("bidfilter");
		
		BoltDeclarer querybolt = builder.setBolt("query", new QueryBolt(), 1);
		querybolt.allGrouping("personfilter", "person");
		querybolt.fieldsGrouping("auctionfilter", "auction", new Fields("auction_id"));
		querybolt.fieldsGrouping("currency", "bid", new Fields("auction_id"));
		
		BoltDeclarer sinkbolt = builder.setBolt("sink", new SimpleSink());
		sinkbolt.globalGrouping("query");
		
		Config conf = new Config();
		conf.setDebug(false);
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("nexmark", conf, builder.createTopology());
	}
}
