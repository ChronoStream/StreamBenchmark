package stream.benchmark.tpcch.query3;

import java.util.LinkedList;
import java.util.List;

public class Q3State {

	protected static class OrderState {
		public OrderState() {
			_is_new = false;
			_customer_id = -1;
			_prices = new LinkedList<Double>();
			_o_entry_d = -1;
		}

		boolean _is_new;
		int _customer_id;
		List<Double> _prices;
		long _o_entry_d;
	}

}
