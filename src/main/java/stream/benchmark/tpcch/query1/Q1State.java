package stream.benchmark.tpcch.query1;

public class Q1State {
	protected static class OrderLineState{
		public OrderLineState(int ol_quantity, double ol_amount, long ol_delivery_d){
			_ol_quantity = ol_quantity;
			_ol_amount = ol_amount;
			_ol_delivery_d = ol_delivery_d;
		}
		int _ol_quantity;
		double _ol_amount;
		long _ol_delivery_d;
	}
}
