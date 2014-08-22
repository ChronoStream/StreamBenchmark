package stream.benchmark.driver;

import stream.benchmark.bolt.AuctionFilter;
import stream.benchmark.bolt.BidFilter;
import stream.benchmark.bolt.CurrencyConversion;
import stream.benchmark.bolt.PersonFilter;
import stream.benchmark.bolt.QueryBolt;
import stream.benchmark.bolt.SimpleSink;
import stream.benchmark.spout.NexmarkSpout;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class DriverMain {

	public static void main(String[] args) throws Exception {
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("spout", new NexmarkSpout(), 1);
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
