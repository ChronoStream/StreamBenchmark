package stream.benchmark.tpcch.query2;

public class Q2State {

	protected static class NationState {
		public NationState(String _nation_name, int _region_key) {
			this._nation_name = _nation_name;
			this._region_key = _region_key;
		}

		String _nation_name;
		int _region_key;
	}

	protected static class StockState {
		public StockState(int _w_id, int _quantity) {
			this._w_id = _w_id;
			this._quantity = _quantity;
		}

		int _w_id;
		int _quantity;
	}

	protected static class SupplierState {
		public SupplierState(String _su_name, int _su_nation_id) {
			this._su_name = _su_name;
			this._su_nation_id = _su_nation_id;
		}

		String _su_name;
		int _su_nation_id;
	}

	protected static class ItemState {
		public ItemState(String _item_name, double _price) {
			this._item_name = _item_name;
			this._price = _price;
		}

		String _item_name;
		double _price;
	}
}
