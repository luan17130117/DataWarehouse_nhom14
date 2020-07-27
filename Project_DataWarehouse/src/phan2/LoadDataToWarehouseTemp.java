package phan2;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import connection.database.GetConnection;

public class LoadDataToWarehouseTemp {
	public static void loadDataToWarehouseTemp(String table_staging, String table_warehouse, String db_staging,
			String fieldName) {
		Connection conn = new GetConnection().getConnection("staging");
		String sql = "INSERT INTO  warehouse_temp (s_key, " + fieldName + ") " + "SELECT 0, " + fieldName
				+ " FROM sinhvien";
		try {
			PreparedStatement ps = conn.prepareStatement(sql);
			ps.executeUpdate();
			conn.close();
		} catch (SQLException e) {
			System.out.println("<---> ERROR [Load data to Warehouse temp]: " + e.getMessage());
		}
	}
}
