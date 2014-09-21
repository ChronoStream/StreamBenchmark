package stream.benchmark.tpcc.query;

import java.util.Map;

public class TableState {
	protected static class WarehouseState{
		int _id;
		String _name;
		String _street1;
		String _street2;
		String _city;
		String _state;
		String _zip;
		double _tax;
		double _ytd;
		Map<Integer, DistrictState> _district_states;
	}
	
	protected static class DistrictState{
		int _id;
		String _name;
		String _street1;
		String _street2;
		String _city;
		String _state;
		String _zip;
		double _tax;
		double _ytd;
		int _next_o_id;
		Map<Integer, CustomerState> _customer_states;
	}
	
	protected static class CustomerState{
		int _id;
		String _first;
		String _middle;
		String _last;
		String _phone;
		long _since;
		String _credit;
		HistoryState _history_state;
		OrderState _order_state;
	}
	
	protected static class HistoryState{
		int _warehouse_id;
		int _district_id;
		int _customer_id;
		long _date;
		double _amount;
		String _data;
	}
	
	protected static class OrderState{
		
	}
	
	protected static class NewOrderState{

	}
	
	protected static class StockState{
		int _item_id;
		int _warehouse_id;
		int _quantity;
		
	}
	
	protected static class OrderLineState{
		
	}
	
	protected static class ItemState{
		int _id;
		int _im_id;
		String _name;
		double price;
		String _data;
	}
}
