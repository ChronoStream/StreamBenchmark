package stream.benchmark.tpcc.spout;

import java.util.LinkedList;
import java.util.List;

public class BenchmarkExecutor {
	
	private ScaleParams _scaleParams;
	public BenchmarkExecutor(ScaleParams params){
		_scaleParams = params;
	}
	
	public String generateDeliveryParams(){
        //Return parameters for DELIVERY
        int w_id = makeWarehouseId();
        int o_carrier_id = BenchmarkRandom.getNumber(BenchmarkConstant.MIN_CARRIER_ID, BenchmarkConstant.MAX_CARRIER_ID);
        long ol_delivery_d = System.currentTimeMillis();
        StringBuilder sb = new StringBuilder();
		sb.append(w_id);
		sb.append(",");
		sb.append(o_carrier_id);
		sb.append(",");
		sb.append(ol_delivery_d);
        return sb.toString();
	}
	
	public String generateNewOrderParams(){
        //Return parameters for NEW_ORDER
        int w_id = makeWarehouseId();
        int d_id = makeDistrictId();
        int c_id = makeCustomerId();
        int ol_cnt = BenchmarkRandom.getNumber(BenchmarkConstant.MIN_OL_CNT, BenchmarkConstant.MAX_OL_CNT);
        long o_entry_d = System.currentTimeMillis();

        //1% of transactions roll back
        boolean rollback = false; // FIXME BenchmarkRandom.number(1, 100) == 1
        
        List<Integer> i_ids = new LinkedList<Integer>();
        List<Integer> i_w_ids = new LinkedList<Integer>();
        List<Integer> i_qtys = new LinkedList<Integer>();
        
        for(int i=0; i<ol_cnt; ++i){
        	if(rollback && i+1 == ol_cnt){
                i_ids.add(_scaleParams._numItems + 1);
        	}else{
                i_ids.add(makeItemId());
        	}
            //1% of items are from a remote warehouse
        	boolean remote = (BenchmarkRandom.getNumber(1, 100) == 1);
        	if(_scaleParams._warehouses > 1 && remote){
                i_w_ids.add(BenchmarkRandom.getNumberExcluding(_scaleParams._starting_warehouse, _scaleParams._ending_warehouse, w_id));
        	}else{
                i_w_ids.add(w_id);
        	}
            i_qtys.add(BenchmarkRandom.getNumber(1, BenchmarkConstant.MAX_OL_QUANTITY));
        }
        StringBuilder sb = new StringBuilder();
		sb.append(w_id);
		sb.append(",");
		sb.append(d_id);
		sb.append(",");
		sb.append(c_id);
		sb.append(",");
		sb.append(o_entry_d);
		sb.append(",");
		sb.append("[");
		for(Integer value : i_ids){
			sb.append(value);
			sb.append(";");
		}
		sb.append("]");
		sb.append(",");
		sb.append("[");
		for(Integer value : i_w_ids){
			sb.append(value);
			sb.append(";");
		}
		sb.append("]");
		sb.append(",");
		sb.append("[");
		for(Integer value : i_qtys){
			sb.append(value);
			sb.append(";");
		}
		sb.append("]");
		return sb.toString();
	}
	
	public String generateOrderStatusParams(){
        //Return parameters for ORDER_STATUS
        int w_id = makeWarehouseId();
        int d_id = makeDistrictId();
        String c_last = "";
        int c_id = -1;
        
        //60%: order status by last name
        if (BenchmarkRandom.getNumber(1, 100) <= 60){
            c_last = BenchmarkRandom.makeRandomLastName(_scaleParams._numCustomersPerDistrict);
	}
        //40%: order status by id
        else{
            c_id = makeCustomerId();
        }
        StringBuilder sb = new StringBuilder();
		sb.append(w_id);
		sb.append(",");
		sb.append(d_id);
		sb.append(",");
		sb.append(c_id);
		sb.append(",");
		sb.append(c_last);
        return sb.toString();
	}
	
	public String generatePaymentParams(){
        //Return parameters for PAYMENT
        int x = BenchmarkRandom.getNumber(1, 100);
        int y = BenchmarkRandom.getNumber(1, 100);

        int w_id = makeWarehouseId();
        int d_id = makeDistrictId();
        int c_w_id = -1;
        int c_d_id = -1;
        int c_id = -1;
        String c_last = "";
        double h_amount = BenchmarkRandom.getFixedPoint(2, BenchmarkConstant.MIN_PAYMENT, BenchmarkConstant.MAX_PAYMENT);
        long h_date = System.currentTimeMillis();

        // 85%: paying through own warehouse (or there is only 1 warehouse)
        if (_scaleParams._warehouses == 1 || x <= 85){
            c_w_id = w_id;
            c_d_id = d_id;
        }
        //15%: paying through another warehouse
        else{
            //select in range [1, num_warehouses] excluding w_id
            c_w_id = BenchmarkRandom.getNumberExcluding(_scaleParams._starting_warehouse, _scaleParams._ending_warehouse, w_id);
            c_d_id = makeDistrictId();
        }
        //60%: payment by last name
        if (y <= 60){
            c_last = BenchmarkRandom.makeRandomLastName(_scaleParams._numCustomersPerDistrict);
        }
        //40%: payment by id
        else{
            c_id = makeCustomerId();
        }
        StringBuilder sb = new StringBuilder();
		sb.append(w_id);
		sb.append(",");
		sb.append(d_id);
		sb.append(",");
		sb.append(h_amount);
		sb.append(",");
		sb.append(c_w_id);
		sb.append(",");
		sb.append(c_d_id);
		sb.append(",");
		sb.append(c_id);
		sb.append(",");
		sb.append(c_last);
		sb.append(",");
		sb.append(h_date);
        return sb.toString();
	}
	
    public String generateStockLevelParams(){
        //Returns parameters for STOCK_LEVEL
        int w_id = makeWarehouseId();
        int d_id = makeDistrictId();
        int threshold = BenchmarkRandom.getNumber(BenchmarkConstant.MIN_STOCK_LEVEL_THRESHOLD, BenchmarkConstant.MAX_STOCK_LEVEL_THRESHOLD);
        StringBuilder sb = new StringBuilder();
		sb.append(w_id);
		sb.append(",");
		sb.append(d_id);
		sb.append(",");
		sb.append(threshold);
        return sb.toString();
    }
    
     private int makeWarehouseId(){
        return BenchmarkRandom.getNumber(_scaleParams._starting_warehouse, _scaleParams._ending_warehouse);
    }
    
     private int makeDistrictId(){
        return BenchmarkRandom.getNumber(1, _scaleParams._numDistrictsPerWarehouse);
    }
    
     private int makeCustomerId(){
        return BenchmarkRandom.NURand(1023, 1, _scaleParams._numCustomersPerDistrict);
    }
    
     private int makeItemId(){
        return BenchmarkRandom.NURand(8191, 1, _scaleParams._numItems);
    }
    
}
