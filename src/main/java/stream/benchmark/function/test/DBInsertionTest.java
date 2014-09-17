package stream.benchmark.function.test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;

public class DBInsertionTest {

	public static void main(String[] args) throws Exception {
		Connection connection = DriverManager.getConnection(
				"jdbc:mysql://localhost:3306/testdb", "root", "");
		Statement statement = connection.createStatement();
		statement.executeUpdate("drop table insertiontest");
		statement
				.executeUpdate("create table insertiontest(mykey varchar(20), myvalue varchar(20), primary key (mykey))");
		PreparedStatement insertion = connection
				.prepareStatement("insert into insertiontest values(?, ?)");
		insertion.setString(1, "hello");
		insertion.setString(2, "world");
		insertion.addBatch();
		insertion.setString(1, "nihao");
		insertion.setString(2, "shijie");
		insertion.addBatch();
		insertion.executeBatch();
		insertion.setString(1, "wu");
		insertion.setString(2, "yingjun");
		insertion.addBatch();
		insertion.executeBatch();
		statement.close();
		connection.close();
	}

}
