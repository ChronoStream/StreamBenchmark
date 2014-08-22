package stream.benchmark.functiontest;

import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.TreeMap;

import stream.benchmark.bolt.QueryBolt.BuyerPrice;
import stream.benchmark.bolt.QueryBolt.ItemPrice;

public class ComputeTest {
	public static class BuyerPrice {
		public BuyerPrice(String buyer_id, int price) {
			this.buyer_id = buyer_id;
			this.price = price;
		}

		public String buyer_id;
		public int price;
	}

	public static class ItemPrice {
		public ItemPrice(String item_id, int price) {
			this.item_id = item_id;
			this.price = price;
		}

		public String item_id;
		public int price;
	}

	public static class ValueComparator implements Comparator<String> {
		Map<String, Integer> base;

		public ValueComparator(Map<String, Integer> base) {
			this.base = base;
		}

		// Note: this comparator imposes orderings that are inconsistent with
		// equals.
		public int compare(String a, String b) {
			if (base.get(a) >= base.get(b)) {
				return 1;
			} else {
				return -1;
			} // returning 0 would merge keys
		}
	}

	public static void main(String[] args) {

		Map<String, LinkedList<BuyerPrice>> itemPrices = new HashMap<String, LinkedList<BuyerPrice>>();
		Map<String, LinkedList<ItemPrice>> buyerPrices = new HashMap<String, LinkedList<ItemPrice>>();
		itemPrices.put("book", new LinkedList<BuyerPrice>());
		itemPrices.get("book").add(new BuyerPrice("aa", 100));
		itemPrices.get("book").add(new BuyerPrice("bb", 50));

		itemPrices.put("sport", new LinkedList<BuyerPrice>());
		itemPrices.get("sport").add(new BuyerPrice("cc", 120));
		itemPrices.get("sport").add(new BuyerPrice("dd", 70));

		itemPrices.put("pen", new LinkedList<BuyerPrice>());
		itemPrices.get("pen").add(new BuyerPrice("ee", 100));
		itemPrices.get("pen").add(new BuyerPrice("ff", 20));

		buyerPrices.put("aa", new LinkedList<ItemPrice>());
		buyerPrices.get("aa").add(new ItemPrice("book", 100));
		buyerPrices.get("aa").add(new ItemPrice("sport", 200));
		buyerPrices.get("aa").add(new ItemPrice("pen", 150));

		buyerPrices.put("bb", new LinkedList<ItemPrice>());
		buyerPrices.get("bb").add(new ItemPrice("book", 110));
		buyerPrices.get("bb").add(new ItemPrice("sport", 100));
		buyerPrices.get("bb").add(new ItemPrice("pen", 150));

		buyerPrices.put("cc", new LinkedList<ItemPrice>());
		buyerPrices.get("cc").add(new ItemPrice("book", 200));
		buyerPrices.get("cc").add(new ItemPrice("sport", 50));
		buyerPrices.get("cc").add(new ItemPrice("pen", 120));

		HashMap<String, Integer> itemAvgPrice = new HashMap<String, Integer>();
		// calculate average price of each item
		for (String item : itemPrices.keySet()) {
			int average = 0;
			for (BuyerPrice priceinfo : itemPrices.get(item)) {
				average += priceinfo.price;
			}
			average = average / itemPrices.get(item).size();
			itemAvgPrice.put(item, average);
		}
		ValueComparator itemvc = new ValueComparator(itemAvgPrice);
		TreeMap<String, Integer> sortedItemAvgPrice = new TreeMap<String, Integer>(
				itemvc);
		sortedItemAvgPrice.putAll(itemAvgPrice);
		for (String key : sortedItemAvgPrice.keySet()) {
			System.out.println(key + ", " + sortedItemAvgPrice.get(key));
		}
		HashMap<String, Integer> buyerAvgPrice = new HashMap<String, Integer>();
		for (String buyer : buyerPrices.keySet()) {
			int average = 0;
			for (ItemPrice priceinfo : buyerPrices.get(buyer)) {
				average += priceinfo.price;
			}
			average = average / buyerPrices.get(buyer).size();
			buyerAvgPrice.put(buyer, average);
		}
		for (String key : buyerAvgPrice.keySet()) {
			System.out.println(key + ", " + buyerAvgPrice.get(key));
		}
		ValueComparator buyervc = new ValueComparator(buyerAvgPrice);
		TreeMap<String, Integer> sortedBuyerAvgPrice = new TreeMap<String, Integer>(
				buyervc);
		sortedBuyerAvgPrice.putAll(buyerAvgPrice);
		for (String key : sortedBuyerAvgPrice.keySet()) {
			System.out.println(key + ", " + sortedBuyerAvgPrice.get(key));
		}
		for (Map.Entry<String, Integer> entry : sortedBuyerAvgPrice.entrySet()) {
			Integer value = entry.getValue();
			String key = entry.getKey();
			System.out.println("key="+key+", value="+value);
		}
		System.out.println(sortedBuyerAvgPrice);
	}

}
