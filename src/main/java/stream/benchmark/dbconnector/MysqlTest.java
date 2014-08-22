package stream.benchmark.dbconnector;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class MysqlTest {

	static Random rand=new Random();
	static final String AB="0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";
	static final int stringLength=20;
	static final int round=50;
	static final int refround=100;
	static final int maxInt=100000;
	
	public static String generateRandomString(){
		StringBuilder sb=new StringBuilder(stringLength);
		for(int i=0; i<stringLength; ++i){
			sb.append(AB.charAt(rand.nextInt(AB.length())));
		}
		return sb.toString();
	}
	
	public static void testDB() throws SQLException{
		Connection connect = null;
		Statement statement = null;
		ResultSet result = null;
		String url = "jdbc:mysql://localhost:3306/testdb";
		String user = "root";
		String password = "";
		String sql;
		connect = DriverManager.getConnection(url, user, password);
		statement = connect.createStatement();
		long startTime, endTime;
		double elapsedSeconds;
		
		statement.executeUpdate("drop table mykeyvalue");
		statement.executeUpdate("create table mykeyvalue(mykey varchar("+String.valueOf(stringLength)+"), myvalue int) engine=memory");
		//insert
		startTime=System.currentTimeMillis();
		for (int i = 0; i < round; ++i) {
			String key = generateRandomString();
			Integer value = rand.nextInt(maxInt);
			sql = "insert into mykeyvalue values('" + key + "'," + value.toString() + ")";
			
			statement.executeUpdate(sql);
		}
		endTime=System.currentTimeMillis();
		elapsedSeconds=(endTime-startTime)/1000.0;

		System.out.println("database insert elapsedTime="+elapsedSeconds+"s");
		//////////////////////////////////
		
		//scan
//		startTime=System.currentTimeMillis();
//		sql = "select avg(myvalue) as mysum from mykeyvalue";
//		result = statement.executeQuery(sql);
//		int mysum = 0;
//		while(result.next()){
//			mysum=result.getInt("mysum");
//			System.out.println("value="+mysum);
//		}
//
//		endTime=System.currentTimeMillis();
//		elapsedSeconds=(endTime-startTime)/1000.0;
//		
//		System.out.println("database scan elapsedTime="+elapsedSeconds+"s");
//		//////////////////////////////////
//		
//		//retrieve scan
//		startTime=System.currentTimeMillis();
//		sql = "select myvalue from mykeyvalue";
//		result = statement.executeQuery(sql);
//		long mysum1 = 0;
//		int count=0;
//		while(result.next()){
//			mysum1+=result.getInt("myvalue");
//			count+=1;
//		}
//		System.out.println("count="+count+", value="+mysum1/count);
//
//		endTime=System.currentTimeMillis();
//		elapsedSeconds=(endTime-startTime)/1000.0;
//		
//		System.out.println("database retrieve scan elapsedTime="+elapsedSeconds+"s");
		
		
		//////////////////////////////////
		
//		result.close();
		statement.close();
		connect.close();
	}
	
	public static void testSqlite() throws SQLException{
		Connection connect = null;
		Statement statement = null;
		ResultSet result = null;
		String sql;
		connect = DriverManager.getConnection("jdbc:sqlite::memory:");
		statement = connect.createStatement();
		long startTime, endTime;
		double elapsedSeconds;
		statement.executeUpdate("create table mykeyvalue(mykey varchar("+String.valueOf(stringLength)+"), myvalue int)");
		//insert
		startTime=System.currentTimeMillis();
		for (int i = 0; i < round; ++i) {
			String key = generateRandomString();
			Integer value = rand.nextInt(maxInt);
			sql = "insert into mykeyvalue values('" + key + "'," + value.toString() + ")";
			
			statement.executeUpdate(sql);
		}
		endTime=System.currentTimeMillis();
		elapsedSeconds=(endTime-startTime)/1000.0;

		System.out.println("database insert elapsedTime="+elapsedSeconds+"s");
		//////////////////////////////////
		
		//scan
		startTime=System.currentTimeMillis();
		sql = "select sum(myvalue) as mysum from mykeyvalue";
		result = statement.executeQuery(sql);
		int mysum = 0;
		while(result.next()){
			mysum=result.getInt("mysum");
			System.out.println("value="+mysum);
		}

		endTime=System.currentTimeMillis();
		elapsedSeconds=(endTime-startTime)/1000.0;
		
		System.out.println("database scan elapsedTime="+elapsedSeconds+"s");
		//////////////////////////////////
		
		//retrieve scan
		startTime=System.currentTimeMillis();
		sql = "select myvalue from mykeyvalue";
		result = statement.executeQuery(sql);
		int mysum1 = 0;
		while(result.next()){
			mysum1+=result.getInt("myvalue");
		}
		System.out.println("value="+mysum1);

		endTime=System.currentTimeMillis();
		elapsedSeconds=(endTime-startTime)/1000.0;
		
		System.out.println("database retrieve scan elapsedTime="+elapsedSeconds+"s");		
		
		
		//////////////////////////////////
		
		result.close();
		statement.close();
		connect.close();
	}
	
	public static void testLib(){
		Map<String, Integer> heapMap = new HashMap<String, Integer>();
		long startTime, endTime;
		double elapsedSeconds;
		
		//insert
		startTime=System.currentTimeMillis();
		for(int i=0; i<round; ++i){
			heapMap.put(generateRandomString(), rand.nextInt(maxInt));
		}
		endTime=System.currentTimeMillis();
		elapsedSeconds=(endTime-startTime)/1000.0;

		System.out.println("library insert elapsedTime="+elapsedSeconds+"s");
		
		final long usedMem = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
		System.out.println("real memory="+(stringLength+4)*round/1024/1024+"MB");
		System.out.println("used memory="+usedMem/1024/1024 +"MB");
		System.out.println("Max memory="+Runtime.getRuntime().maxMemory()/1024/1024+"MB");
		
		//scan
//		startTime=System.currentTimeMillis();
//		int sum=0;
//		for(Integer key : heapMap.values()){
//			sum+=key;
//		}		
//		endTime=System.currentTimeMillis();
//		elapsedSeconds=(endTime-startTime)/1000.0;
//		
//		System.out.println("sum="+sum+", library scan elapsedTime="+elapsedSeconds+"s");
	}
	
	public static void sqliteJoin() throws SQLException{
		Connection connect = null;
		Statement statement = null;
		ResultSet result = null;
		String sql;
		connect = DriverManager.getConnection("jdbc:sqlite::memory:");
		statement = connect.createStatement();
		long startTime, endTime;
		double elapsedSeconds;
		
		statement.executeUpdate("create table mykeyvalue0(mykey varchar("+String.valueOf(stringLength)+"), myvalue int)");
		//insert
		startTime=System.currentTimeMillis();
		for (int i = 0; i < round; ++i) {
			String key = generateRandomString();
			Integer value = rand.nextInt(maxInt);
			sql = "insert into mykeyvalue0 values('" + key + "'," + value.toString() + ")";
			
			statement.executeUpdate(sql);
		}
		endTime=System.currentTimeMillis();
		elapsedSeconds=(endTime-startTime)/1000.0;
		System.out.println("database insert elapsedTime="+elapsedSeconds+"s");
		//////////////////////////////////
		
		statement.executeUpdate("create table mykeyvalue1(mykey varchar("+String.valueOf(stringLength)+"), myvalue int)");
		//insert
		startTime=System.currentTimeMillis();
		for (int i = 0; i < refround; ++i) {
			String key = generateRandomString();
			Integer value = rand.nextInt(maxInt);
			sql = "insert into mykeyvalue1 values('" + key + "'," + value.toString() + ")";
			
			statement.executeUpdate(sql);
		}
		endTime=System.currentTimeMillis();
		elapsedSeconds=(endTime-startTime)/1000.0;		
		System.out.println("database insert elapsedTime="+elapsedSeconds+"s");
		//////////////////////////////////
		
		//join
		startTime=System.currentTimeMillis();
		sql = "select mykeyvalue1.mykey as resultkey from mykeyvalue0 inner join mykeyvalue1 on mykeyvalue0.mykey=mykeyvalue1.mykey";
		result = statement.executeQuery(sql);
		while(result.next()){
			result.getString("resultkey");
		}
		endTime=System.currentTimeMillis();
		elapsedSeconds=(endTime-startTime)/1000.0;
		
		System.out.println("database join elapsedTime="+elapsedSeconds+"s");
		//////////////////////////////////
		
		result.close();
		statement.close();
		connect.close();
	}
	
	public static void sqliteExternalJoin() throws SQLException{
		Connection connect = null;
		Statement statement = null;
		ResultSet result = null;
		String sql;
		connect = DriverManager.getConnection("jdbc:sqlite::memory:");
		statement = connect.createStatement();
		long startTime, endTime;
		double elapsedSeconds;
		
		statement.executeUpdate("create table mykeyvalue0(mykey varchar("+String.valueOf(stringLength)+"), myvalue int)");
		//insert
		startTime=System.currentTimeMillis();
		for (int i = 0; i < round; ++i) {
			String key = generateRandomString();
			Integer value = rand.nextInt(maxInt);
			sql = "insert into mykeyvalue0 values('" + key + "'," + value.toString() + ")";
			
			statement.executeUpdate(sql);
		}
		endTime=System.currentTimeMillis();
		elapsedSeconds=(endTime-startTime)/1000.0;
		System.out.println("database insert elapsedTime="+elapsedSeconds+"s");
		//////////////////////////////////
		
		Map<String, Integer> heapMap = new HashMap<String, Integer>();
		//insert
		startTime=System.currentTimeMillis();
		for(int i=0; i<refround; ++i){
			heapMap.put(generateRandomString(), rand.nextInt(maxInt));
		}
		endTime=System.currentTimeMillis();
		elapsedSeconds=(endTime-startTime)/1000.0;

		System.out.println("library insert elapsedTime="+elapsedSeconds+"s");
		//////////////////////////////////
		
		//join
		startTime = System.currentTimeMillis();
		StringBuilder sb=new StringBuilder();
		for (String heapkey : heapMap.keySet()) {
			sb.append("'"+heapkey+"',");
		}
		sb.deleteCharAt(sb.length()-1);
		sql = "select mykeyvalue0.mykey as resultkey from mykeyvalue0 where mykeyvalue0.mykey in ("+sb.toString()+")";
		result = statement.executeQuery(sql);
		while (result.next()) {
			result.getString("resultkey");
		}
		endTime=System.currentTimeMillis();
		elapsedSeconds=(endTime-startTime)/1000.0;		
		System.out.println("database array join elapsedTime="+elapsedSeconds+"s");
		//////////////////////////////////
		
		result.close();
		statement.close();
		connect.close();		
	}
	
	
	public static void mysqlJoin() throws SQLException{
		Connection connect = null;
		Statement statement = null;
		ResultSet result = null;
		String url = "jdbc:mysql://localhost:3306/testdb";
		String user = "root";
		String password = "yingjun";
		String sql;
		connect = DriverManager.getConnection(url, user, password);
		statement = connect.createStatement();
		long startTime, endTime;
		double elapsedSeconds;
		
//		statement.executeUpdate("drop table mykeyvalue0");
		statement.executeUpdate("drop table mykeyvalue1");
//		statement.executeUpdate("create table mykeyvalue0(mykey varchar("+String.valueOf(stringLength)+"), myvalue int, index using hash (mykey)) engine=memory");
//		//insert
//		startTime=System.currentTimeMillis();
//		for (int i = 0; i < round; ++i) {
//			String key = generateRandomString();
//			Integer value = rand.nextInt(maxInt);
//			sql = "insert into mykeyvalue0 values('" + key + "'," + value.toString() + ")";
//			
//			statement.executeUpdate(sql);
//		}
//		endTime=System.currentTimeMillis();
//		elapsedSeconds=(endTime-startTime)/1000.0;
//		System.out.println("database insert elapsedTime="+elapsedSeconds+"s");
//		//////////////////////////////////
		
		statement.executeUpdate("create table mykeyvalue1(mykey varchar("+String.valueOf(stringLength)+"), myvalue int, index using hash (mykey)) engine=memory");
		//insert
		startTime=System.currentTimeMillis();
		for (int i = 0; i < refround; ++i) {
			String key = generateRandomString();
			Integer value = rand.nextInt(maxInt);
			sql = "insert into mykeyvalue1 values('" + key + "'," + value.toString() + ")";
			
			statement.executeUpdate(sql);
		}
		endTime=System.currentTimeMillis();
		elapsedSeconds=(endTime-startTime)/1000.0;		
		System.out.println("database insert elapsedTime="+elapsedSeconds+"s");
		//////////////////////////////////
		
		//join
		startTime=System.currentTimeMillis();
		sql = "select mykeyvalue1.mykey as resultkey from mykeyvalue0 inner join mykeyvalue1 on mykeyvalue0.mykey=mykeyvalue1.mykey";
		result = statement.executeQuery(sql);
		while(result.next()){
			result.getString("resultkey");
		}
		endTime=System.currentTimeMillis();
		elapsedSeconds=(endTime-startTime)/1000.0;
		
		System.out.println("database join elapsedTime="+elapsedSeconds+"s");
		//////////////////////////////////
		
		result.close();
		statement.close();
		connect.close();
	}
	
	public static void mysqlExternalJoin() throws SQLException{
		Connection connect = null;
		Statement statement = null;
		ResultSet result = null;
		String url = "jdbc:mysql://localhost:3306/testdb";
		String user = "root";
		String password = "yingjun";
		String sql;
		connect = DriverManager.getConnection(url, user, password);
		statement = connect.createStatement();
		long startTime, endTime;
		double elapsedSeconds;
		
//		statement.executeUpdate("drop table mykeyvalue0");
//		statement.executeUpdate("create table mykeyvalue0(mykey varchar("+String.valueOf(stringLength)+"), myvalue int) engine=memory");
//		//insert
//		startTime=System.currentTimeMillis();
//		for (int i = 0; i < round; ++i) {
//			String key = generateRandomString();
//			Integer value = rand.nextInt(maxInt);
//			sql = "insert into mykeyvalue0 values('" + key + "'," + value.toString() + ")";
//			
//			statement.executeUpdate(sql);
//		}
//		endTime=System.currentTimeMillis();
//		elapsedSeconds=(endTime-startTime)/1000.0;
//		System.out.println("database insert elapsedTime="+elapsedSeconds+"s");
//		//////////////////////////////////
		
		Map<String, Integer> heapMap = new HashMap<String, Integer>();
		//insert
		startTime=System.currentTimeMillis();
		for(int i=0; i<refround; ++i){
			heapMap.put(generateRandomString(), rand.nextInt(maxInt));
		}
		endTime=System.currentTimeMillis();
		elapsedSeconds=(endTime-startTime)/1000.0;

		System.out.println("library insert elapsedTime="+elapsedSeconds+"s");
		//////////////////////////////////
		
		//join
		startTime = System.currentTimeMillis();
		StringBuilder sb=new StringBuilder();
		for (String heapkey : heapMap.keySet()) {
			sb.append("'"+heapkey+"',");
		}
		sb.deleteCharAt(sb.length()-1);
		sql = "select mykeyvalue0.mykey as resultkey from mykeyvalue0 where mykeyvalue0.mykey in ("+sb.toString()+")";
		result = statement.executeQuery(sql);
		while (result.next()) {
			result.getString("resultkey");
		}
		endTime=System.currentTimeMillis();
		elapsedSeconds=(endTime-startTime)/1000.0;		
		System.out.println("database array join elapsedTime="+elapsedSeconds+"s");
		//////////////////////////////////
		
		result.close();
		statement.close();
		connect.close();		
	}
	
	public static void testUsage(){
		
	}
	
	public static void main(String[] args) throws Exception{
		//testLib();
		testDB();
		//testSqlite();
		//externalJoin();
		//sqliteJoin();
		//mysqlExternalJoin();
		//System.out.println("===================");
		//mysqlJoin();
		
	}

}
