package stream.benchmark.tpcc.spout;

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

	// warehouse_tax, warehouse_ytd, warehouse_address
	static public String generateWarehouse(int w_id) {
		double w_tax = generateTax();
		double w_ytd = BenchmarkConstant.INITIAL_W_YTD;
		String[] w_address = generateAddress();
		StringBuilder sb = new StringBuilder();
		sb.append(w_id);
		sb.append(",");
		for(String str : w_address){
			sb.append(str);
			sb.append(",");
		}
		sb.append(w_tax);
		sb.append(",");
		sb.append(w_ytd);
		return sb.toString();
	}
	
	static public String generateDistrict(int d_w_id, int d_id, int d_next_o_id){
		double d_tax = generateTax();
		double d_ytd = BenchmarkConstant.INITIAL_D_YTD;
		String[] d_address = generateAddress();
		StringBuilder sb = new StringBuilder();
		sb.append(d_id);
		sb.append(",");
		sb.append(d_w_id);
		sb.append(",");
		for(String str : d_address){
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

	static private double generateTax() {
		return BenchmarkRandom.getFixedPoint(BenchmarkConstant.TAX_DECIMALS,
				BenchmarkConstant.MIN_TAX, BenchmarkConstant.MAX_TAX);
	}
	
	static private String[] generateAddress(){
		String[] address=new String[6];
		//name
		address[0] = BenchmarkRandom.getAstring(BenchmarkConstant.MIN_NAME, BenchmarkConstant.MAX_NAME);
        //street1
		address[1] = BenchmarkRandom.getAstring(BenchmarkConstant.MIN_STREET, BenchmarkConstant.MAX_STREET);
        //street2
		address[2] = BenchmarkRandom.getAstring(BenchmarkConstant.MIN_STREET, BenchmarkConstant.MAX_STREET);
        //city
		address[3] = BenchmarkRandom.getAstring(BenchmarkConstant.MIN_CITY, BenchmarkConstant.MAX_CITY);
        //state
		address[4] = BenchmarkRandom.getAstring(BenchmarkConstant.STATE, BenchmarkConstant.STATE);
        //zip
		address[5] = generateZip();
		return address;
	}
	
    static private String generateZip(){
        int length = BenchmarkConstant.ZIP_LENGTH - BenchmarkConstant.ZIP_SUFFIX.length();
        return BenchmarkRandom.getNstring(length, length) + BenchmarkConstant.ZIP_SUFFIX;
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
