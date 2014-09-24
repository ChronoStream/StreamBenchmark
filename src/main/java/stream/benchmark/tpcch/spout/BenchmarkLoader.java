package stream.benchmark.tpcch.spout;

import java.util.LinkedList;
import java.util.List;

public class BenchmarkLoader {

	// item_id, item_image_id, item_name, item_price, item_data
	static public String generateItem(int item_id, boolean original) {
		int i_id = item_id;
		int i_im_id = BenchmarkRandom.getNumber(BenchmarkConstant.MIN_IM,
				BenchmarkConstant.MAX_IM);
		String i_name = BenchmarkRandom.getAstring(
				BenchmarkConstant.MIN_I_NAME, BenchmarkConstant.MAX_I_NAME);
		double i_price = BenchmarkRandom.getFixedPoint(
				BenchmarkConstant.MONEY_DECIMALS, BenchmarkConstant.MIN_PRICE,
				BenchmarkConstant.MAX_PRICE);
		String i_data = BenchmarkRandom.getAstring(
				BenchmarkConstant.MIN_I_DATA, BenchmarkConstant.MAX_I_DATA);
		if (original) {
			i_data = fillOriginal(i_data);
		}
		StringBuilder sb = new StringBuilder();
		sb.append(i_id);
		sb.append(",");
		sb.append(i_im_id);
		sb.append(",");
		sb.append(i_name);
		sb.append(",");
		sb.append(i_price);
		sb.append(",");
		sb.append(i_data);
		return sb.toString();
	}

	// warehouse_id, warehouse_name, warehouse_address, warehouse_tax, warehouse_ytd
	static public String generateWarehouse(int w_id) {
		double w_tax = generateTax();
		double w_ytd = BenchmarkConstant.INITIAL_W_YTD;
		String w_name = BenchmarkRandom.getAstring(BenchmarkConstant.MIN_NAME,
				BenchmarkConstant.MAX_NAME);
		String[] w_address = generateAddress();
		StringBuilder sb = new StringBuilder();
		sb.append(w_id);
		sb.append(",");
		sb.append(w_name);
		sb.append(",");
		for (String str : w_address) {
			sb.append(str);
			sb.append(",");
		}
		sb.append(w_tax);
		sb.append(",");
		sb.append(w_ytd);
		return sb.toString();
	}

	static public String generateDistrict(int d_w_id, int d_id, int d_next_o_id) {
		double d_tax = generateTax();
		double d_ytd = BenchmarkConstant.INITIAL_D_YTD;
		String d_name = BenchmarkRandom.getAstring(BenchmarkConstant.MIN_NAME,
				BenchmarkConstant.MAX_NAME);
		String[] d_address = generateAddress();
		StringBuilder sb = new StringBuilder();
		sb.append(d_id);
		sb.append(",");
		sb.append(d_w_id);
		sb.append(",");
		sb.append(d_name);
		sb.append(",");
		for (String str : d_address) {
			sb.append(str);
			sb.append(",");
		}
		sb.append(d_tax);
		sb.append(",");
		sb.append(d_ytd);
		sb.append(",");
		sb.append(d_next_o_id);
		return sb.toString();
	}

	static public String generateCustomer(int c_w_id, int c_d_id, int c_id,
			boolean badCredit) {
		String c_first = BenchmarkRandom.getAstring(
				BenchmarkConstant.MIN_FIRST, BenchmarkConstant.MAX_FIRST);
		String c_middle = BenchmarkConstant.MIDDLE;
		String c_last;
		if (c_id <= 1000) {
			c_last = BenchmarkRandom.makeLastName(c_id - 1);
		} else {
			c_last = BenchmarkRandom
					.makeRandomLastName(BenchmarkConstant.CUSTOMERS_PER_DISTRICT);
		}
		String c_phone = BenchmarkRandom.getNstring(BenchmarkConstant.PHONE,
				BenchmarkConstant.PHONE);
		long c_since = System.currentTimeMillis();
		String c_credit = BenchmarkConstant.GOOD_CREDIT;
		if (badCredit) {
			c_credit = BenchmarkConstant.BAD_CREDIT;
		}
		double c_credit_lim = BenchmarkConstant.INITIAL_CREDIT_LIM;
		double c_discount = BenchmarkRandom.getFixedPoint(
				BenchmarkConstant.DISCOUNT_DECIMALS,
				BenchmarkConstant.MIN_DISCOUNT, BenchmarkConstant.MAX_DISCOUNT);
		double c_balance = BenchmarkConstant.INITIAL_BALANCE;
		double c_ytd_payment = BenchmarkConstant.INITIAL_YTD_PAYMENT;
		int c_payment_cnt = BenchmarkConstant.INITIAL_PAYMENT_CNT;
		int c_delivery_cnt = BenchmarkConstant.INITIAL_DELIVERY_CNT;
		String c_data = BenchmarkRandom.getAstring(
				BenchmarkConstant.MIN_C_DATA, BenchmarkConstant.MAX_C_DATA);
		String[] c_address = generateAddress();
		StringBuilder sb = new StringBuilder();
		sb.append(c_id);
		sb.append(",");
		sb.append(c_d_id);
		sb.append(",");
		sb.append(c_w_id);
		sb.append(",");
		sb.append(c_first);
		sb.append(",");
		sb.append(c_middle);
		sb.append(",");
		sb.append(c_last);
		sb.append(",");
		for (String str : c_address) {
			sb.append(str);
			sb.append(",");
		}
		sb.append(c_phone);
		sb.append(",");
		sb.append(c_since);
		sb.append(",");
		sb.append(c_credit);
		sb.append(",");
		sb.append(c_credit_lim);
		sb.append(",");
		sb.append(c_discount);
		sb.append(",");
		sb.append(c_balance);
		sb.append(",");
		sb.append(c_ytd_payment);
		sb.append(",");
		sb.append(c_payment_cnt);
		sb.append(",");
		sb.append(c_delivery_cnt);
		sb.append(",");
		sb.append(c_data);
		return sb.toString();
	}

	static public String generateOrder(int o_w_id, int o_d_id, int o_id,
			int o_c_id, int o_ol_cnt, boolean newOrder) {
		long o_entry_d = System.currentTimeMillis();
		long o_carrier_id = BenchmarkRandom.getNumber(
				BenchmarkConstant.MIN_CARRIER_ID,
				BenchmarkConstant.MAX_CARRIER_ID);
		if (newOrder) {
			o_carrier_id = BenchmarkConstant.NULL_CARRIER_ID;
		}
		boolean o_all_local = BenchmarkConstant.INITIAL_ALL_LOCAL;

		StringBuilder sb = new StringBuilder();
		sb.append(o_id);
		sb.append(",");
		sb.append(o_c_id);
		sb.append(",");
		sb.append(o_d_id);
		sb.append(",");
		sb.append(o_w_id);
		sb.append(",");
		sb.append(o_entry_d);
		sb.append(",");
		sb.append(o_carrier_id);
		sb.append(",");
		sb.append(o_ol_cnt);
		sb.append(",");
		sb.append(o_all_local);
		return sb.toString();
	}

	static public String generateOrderLine(int ol_w_id, int ol_d_id, int ol_o_id,
			int ol_number, int max_items, boolean newOrder) {
		int ol_i_id = BenchmarkRandom.getNumber(1, max_items);
		int ol_supply_w_id = ol_w_id;
		long ol_delivery_d = System.currentTimeMillis();
		int ol_quantity = BenchmarkConstant.INITIAL_QUANTITY;

		double ol_amount;
		if (newOrder == false) {
			ol_amount = 0.00;
		} else {
			ol_amount = BenchmarkRandom.getFixedPoint(
					BenchmarkConstant.MONEY_DECIMALS,
					BenchmarkConstant.MIN_AMOUNT, BenchmarkConstant.MAX_PRICE
							* BenchmarkConstant.MAX_OL_QUANTITY);
			ol_delivery_d = -1;
		}
		String ol_dist_info = BenchmarkRandom.getAstring(
				BenchmarkConstant.DIST, BenchmarkConstant.DIST);
		StringBuilder sb = new StringBuilder();
		sb.append(ol_o_id);
		sb.append(",");
		sb.append(ol_d_id);
		sb.append(",");
		sb.append(ol_w_id);
		sb.append(",");
		sb.append(ol_number);
		sb.append(",");
		sb.append(ol_i_id);
		sb.append(",");
		sb.append(ol_supply_w_id);
		sb.append(",");
		sb.append(ol_delivery_d);
		sb.append(",");
		sb.append(ol_quantity);
		sb.append(",");
		sb.append(ol_amount);
		sb.append(",");
		sb.append(ol_dist_info);
		return sb.toString();
	}

	static public String generateStock(int s_w_id, int s_i_id, boolean original) {
		int s_quantity = BenchmarkRandom.getNumber(
				BenchmarkConstant.MIN_QUANTITY, BenchmarkConstant.MAX_QUANTITY);
		int s_ytd = 0;
		int s_order_cnt = 0;
		int s_remote_cnt = 0;

		String s_data = BenchmarkRandom.getAstring(
				BenchmarkConstant.MIN_I_DATA, BenchmarkConstant.MAX_I_DATA);
		if (original) {
			fillOriginal(s_data);
		}

		List<String> s_dists = new LinkedList<String>();
		for (int i = 0; i < BenchmarkConstant.DISTRICTS_PER_WAREHOUSE; ++i) {
			s_dists.add(BenchmarkRandom.getAstring(BenchmarkConstant.DIST,
					BenchmarkConstant.DIST));
		}

		StringBuilder sb = new StringBuilder();
		sb.append(s_i_id);
		sb.append(",");
		sb.append(s_w_id);
		sb.append(",");
		sb.append(s_quantity);
		sb.append(",");
		for (String dist : s_dists) {
			sb.append(dist);
			sb.append(",");
		}
		sb.append(s_ytd);
		sb.append(",");
		sb.append(s_order_cnt);
		sb.append(",");
		sb.append(s_remote_cnt);
		sb.append(",");
		sb.append(s_data);
		return sb.toString();
	}

	static public String generateHistory(int h_c_w_id, int h_c_d_id, int h_c_id) {
		int h_w_id = h_c_w_id;
		int h_d_id = h_c_d_id;
		long h_date = System.currentTimeMillis();
		double h_amount = BenchmarkConstant.INITIAL_AMOUNT;
		String h_data = BenchmarkRandom.getAstring(BenchmarkConstant.MIN_DATA,
				BenchmarkConstant.MAX_DATA);
		StringBuilder sb = new StringBuilder();
		sb.append(h_c_id);
		sb.append(",");
		sb.append(h_c_d_id);
		sb.append(",");
		sb.append(h_c_w_id);
		sb.append(",");
		sb.append(h_d_id);
		sb.append(",");
		sb.append(h_w_id);
		sb.append(",");
		sb.append(h_date);
		sb.append(",");
		sb.append(h_amount);
		sb.append(",");
		sb.append(h_data);
		return sb.toString();
	}

	static private double generateTax() {
		return BenchmarkRandom.getFixedPoint(BenchmarkConstant.TAX_DECIMALS,
				BenchmarkConstant.MIN_TAX, BenchmarkConstant.MAX_TAX);
	}

	static private String[] generateAddress() {
		String[] address = new String[5];
		// street1
		address[0] = BenchmarkRandom.getAstring(BenchmarkConstant.MIN_STREET,
				BenchmarkConstant.MAX_STREET);
		// street2
		address[1] = BenchmarkRandom.getAstring(BenchmarkConstant.MIN_STREET,
				BenchmarkConstant.MAX_STREET);
		// city
		address[2] = BenchmarkRandom.getAstring(BenchmarkConstant.MIN_CITY,
				BenchmarkConstant.MAX_CITY);
		// state
		address[3] = BenchmarkRandom.getAstring(BenchmarkConstant.STATE,
				BenchmarkConstant.STATE);
		// zip
		address[4] = generateZip();
		return address;
	}

	static private String generateZip() {
		int length = BenchmarkConstant.ZIP_LENGTH
				- BenchmarkConstant.ZIP_SUFFIX.length();
		return BenchmarkRandom.getNstring(length, length)
				+ BenchmarkConstant.ZIP_SUFFIX;
	}

	// ==============================================
	// fillOriginal
	// ==============================================
	static private String fillOriginal(String data) {
		// a string with ORIGINAL_STRING at a random position
		int originalLength = BenchmarkConstant.ORIGINAL_STRING.length();
		int position = BenchmarkRandom.getNumber(0, data.length()
				- originalLength);
		String out = data.substring(0, position)
				+ BenchmarkConstant.ORIGINAL_STRING
				+ data.substring(position + originalLength);
		return out;
	}
}
