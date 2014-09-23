package stream.benchmark.tpcc.query;

import java.util.List;

public class TableState {
	protected static class WarehouseState {
		public WarehouseState(int _id, String _name, String _street1,
				String _street2, String _city, String _state, String _zip,
				double _tax, double _ytd) {
			this._id = _id;
			this._name = _name;
			this._street1 = _street1;
			this._street2 = _street2;
			this._city = _city;
			this._state = _state;
			this._zip = _zip;
			this._tax = _tax;
			this._ytd = _ytd;
		}

		int _id;
		String _name;
		String _street1;
		String _street2;
		String _city;
		String _state;
		String _zip;
		double _tax;
		double _ytd;
	}

	protected static class DistrictState {
		public DistrictState(int _id, int _w_id, String _name, String _street1,
				String _street2, String _city, String _state, String _zip,
				double _tax, double _ytd, int _next_o_id) {
			this._id = _id;
			this._w_id = _w_id;
			this._name = _name;
			this._street1 = _street1;
			this._street2 = _street2;
			this._city = _city;
			this._state = _state;
			this._zip = _zip;
			this._tax = _tax;
			this._ytd = _ytd;
			this._next_o_id = _next_o_id;
		}

		int _id;
		int _w_id;
		String _name;
		String _street1;
		String _street2;
		String _city;
		String _state;
		String _zip;
		double _tax;
		double _ytd;
		int _next_o_id;
	}

	protected static class CustomerState {
		public CustomerState(int _id, int _d_id, int _w_id, String _first,
				String _middle, String _last, String _street1, String _street2,
				String _city, String _state, String _zip, String _phone,
				long _since, String _credit, double _credit_lim,
				double _discount, double _balance, double _ytd_payment,
				int _payment_count, int _delivery_count, String _data) {
			this._id = _id;
			this._d_id = _d_id;
			this._w_id = _w_id;
			this._first = _first;
			this._middle = _middle;
			this._last = _last;
			this._street1 = _street1;
			this._street2 = _street2;
			this._city = _city;
			this._state = _state;
			this._zip = _zip;
			this._phone = _phone;
			this._since = _since;
			this._credit = _credit;
			this._credit_lim = _credit_lim;
			this._discount = _discount;
			this._balance = _balance;
			this._ytd_payment = _ytd_payment;
			this._payment_count = _payment_count;
			this._delivery_count = _delivery_count;
			this._data = _data;
		}

		int _id;
		int _d_id;
		int _w_id;
		String _first;
		String _middle;
		String _last;
		String _street1;
		String _street2;
		String _city;
		String _state;
		String _zip;
		String _phone;
		long _since;
		String _credit;
		double _credit_lim;
		double _discount;
		double _balance;
		double _ytd_payment;
		int _payment_count;
		int _delivery_count;
		String _data;
	}

	protected static class HistoryState {
		public HistoryState(int _c_id, int _c_d_id, int _c_w_id, int _d_id,
				int _w_id, long _date, double _amount, String _data) {
			this._c_id = _c_id;
			this._c_d_id = _c_d_id;
			this._c_w_id = _c_w_id;
			this._d_id = _d_id;
			this._w_id = _w_id;
			this._date = _date;
			this._amount = _amount;
			this._data = _data;
		}

		int _c_id;
		int _c_d_id;
		int _c_w_id;
		int _d_id;
		int _w_id;
		long _date;
		double _amount;
		String _data;
	}

	protected static class OrderState {
		public OrderState(int _id, int _c_id, int _d_id, int _w_id,
				long _entry_d, int _carrier_id, double _ol_cnt,
				boolean _all_local) {
			this._id = _id;
			this._c_id = _c_id;
			this._d_id = _d_id;
			this._w_id = _w_id;
			this._entry_d = _entry_d;
			this._carrier_id = _carrier_id;
			this._ol_cnt = _ol_cnt;
			this._all_local = _all_local;
		}

		int _id;
		int _c_id;
		int _d_id;
		int _w_id;
		long _entry_d;
		int _carrier_id;
		double _ol_cnt;
		boolean _all_local;
	}

	protected static class NewOrderState {
		public NewOrderState(int _o_id, int _d_id, int _w_id) {
			this._o_id = _o_id;
			this._d_id = _d_id;
			this._w_id = _w_id;
		}

		int _o_id;
		int _d_id;
		int _w_id;
	}

	protected static class StockState {
		public StockState(int _i_id, int _w_id, int _quantity,
				List<String> _dists, int _ytd, int _order_cnt, int _remote_cnt,
				String _data) {
			this._i_id = _i_id;
			this._w_id = _w_id;
			this._quantity = _quantity;
			this._dists = _dists;
			this._ytd = _ytd;
			this._order_cnt = _order_cnt;
			this._remote_cnt = _remote_cnt;
			this._data = _data;
		}
		int _i_id;
		int _w_id;
		int _quantity;
		List<String> _dists;
		int _ytd;
		int _order_cnt;
		int _remote_cnt;
		String _data;
	}

	protected static class OrderLineState {
		public OrderLineState(int _ol_o_id, int _ol_d_id, int _ol_w_id,
				int _ol_number, int _ol_i_id, int _ol_supply_w_id,
				long _ol_delivery_d, int _ol_quantity, double _ol_amount,
				String _ol_dist_info) {
			this._ol_o_id = _ol_o_id;
			this._ol_d_id = _ol_d_id;
			this._ol_w_id = _ol_w_id;
			this._ol_number = _ol_number;
			this._ol_i_id = _ol_i_id;
			this._ol_supply_w_id = _ol_supply_w_id;
			this._ol_delivery_d = _ol_delivery_d;
			this._ol_quantity = _ol_quantity;
			this._ol_amount = _ol_amount;
			this._ol_dist_info = _ol_dist_info;
		}

		int _ol_o_id;
		int _ol_d_id;
		int _ol_w_id;
		int _ol_number;
		int _ol_i_id;
		int _ol_supply_w_id;
		long _ol_delivery_d;
		int _ol_quantity;
		double _ol_amount;
		String _ol_dist_info;
	}

	protected static class ItemState {
		public ItemState(int _id, int _im_id, String _name, double _price,
				String _data) {
			this._id = _id;
			this._im_id = _im_id;
			this._name = _name;
			this._price = _price;
			this._data = _data;
		}

		int _id;
		int _im_id;
		String _name;
		double _price;
		String _data;
	}
}
