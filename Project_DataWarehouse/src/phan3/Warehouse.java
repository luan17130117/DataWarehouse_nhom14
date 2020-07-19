package phan3;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.NoSuchElementException;
import java.util.StringTokenizer;
import java.util.regex.Pattern;

import com.chilkatsoft.CkGlobal;
import com.chilkatsoft.CkScp;
import com.chilkatsoft.CkSsh;

public class Warehouse {
	private static Connection con, conStaging, conWarehouse;
	static String hostName;
	static int port;
	static String userName;
	static String passWord;
	static String remotePath;
	static String localPath;

	static {
		try {
			System.loadLibrary("chilkat"); // copy file chilkat.dll vao thu muc bin trong jdk
		} catch (UnsatisfiedLinkError e) {
			System.err.println("Native code library failed to load.\n" + e);
			System.exit(1);
		}
	}

	public static void createConnection() throws SQLException {
		System.out.println("Connecting database....");
		try {
//			Class.forName("com.mysql.jdbc.Driver");
			con = DriverManager.getConnection(
					"jdbc:mysql://localhost:3306/dataWH?useUnicode=true&characterEncoding=utf-8", "root", "");
			conStaging = DriverManager.getConnection(
					"jdbc:mysql://localhost:3306/staging?useUnicode=true&characterEncoding=utf-8", "root", "");
			conWarehouse = DriverManager.getConnection(
					"jdbc:mysql://localhost:3306/warehouse?useUnicode=true&characterEncoding=utf-8", "root", "");

			System.out.println("Complete!!!!!");
			System.out.println("----------------------------------------------------------------");
		} catch (SQLException e) {
			System.out.println("Can't connect!!!!!!!!!!");
			System.out.println("----------------------------------------------------------------");
		}
	}

	public void downloadFile() {
		CkSsh ssh = new CkSsh();
		CkGlobal ck = new CkGlobal();
		ck.UnlockBundle("Hello Admin");
		boolean success = ssh.Connect(hostName, port);
		if (success != true) {
			System.out.println(ssh.lastErrorText());
			return;
		}

		ssh.put_IdleTimeoutMs(5000);
		success = ssh.AuthenticatePw(userName, passWord);
		if (success != true) {
			System.out.println(ssh.lastErrorText());
			return;
		}
		CkScp scp = new CkScp();
		success = scp.UseSsh(ssh);
		if (success != true) {
			System.out.println(scp.lastErrorText());
			return;
		}
		scp.put_SyncMustMatch("");// down tat ca cac file bat dau bang sinhvien
		success = scp.SyncTreeDownload(remotePath, localPath, 2, false);
		if (success != true) {
			System.out.println(scp.lastErrorText());
			System.out.println("tải thanh cong");
			return;
		}
		ssh.Disconnect();
	}

	public void getConfig() throws SQLException {
		// prepareStatement thực hiện câu query
		PreparedStatement st = con.prepareStatement("SELECT * from config");
		// resulSet chứa dữ liệu
		ResultSet rs = st.executeQuery();
		rs.next(); // để result chạy
		hostName = rs.getString("hostName");
		port = rs.getInt("port");
		userName = rs.getString("user");
		passWord = rs.getString("password");
		remotePath = rs.getString("remotePath");
		localPath = rs.getString("localPath");
		System.out.println("lay dlieu thanh cong");

	}

	public void wiriteLog(String path) throws SQLException, IOException {
		Warehouse cl = new Warehouse();
		System.out.println("ChilkatExample");
		File file = new File(path);
		System.out.println("File");
		try {
//			BufferedReader br = new BufferedReader(new FileReader(file));
			if (file.isDirectory()) {

				File[] listFile = file.listFiles();
				for (int i = 0; i < listFile.length; i++) {
					int numberOfLine = readLine(listFile[i]);
					cl.setupLog(listFile[i].getName(), "ER", numberOfLine);

				}
			} else if (!file.exists()) {
				System.out.println("No fine path");

			}
//			br.close();
			con.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private void setupLog(String name, String status, int numberOfLine) throws SQLException {
		String query = "INSERT INTO log (fileName, dateLoadStaging, `status`, numberOfLine) VALUES (?,?,?,?)";
		PreparedStatement st = con.prepareStatement(query);
		st.setString(1, name);
		st.setString(2, new Timestamp(System.currentTimeMillis()).toString().substring(0, 19));
		st.setString(3, status);
		st.setInt(4, numberOfLine);
		st.execute();

	}

	private int readLine(File fileName) throws IOException {
		int result = 0;
		BufferedReader br = new BufferedReader(new FileReader(fileName));
		if (fileName.getPath().endsWith(".txt")) {
			String line = br.readLine();

			if (Pattern.matches("^[0-9]*$", line.substring(0, 1))) {
				result++;
			}
			while ((line = br.readLine()) != null) {
				if (!line.trim().isEmpty())
					result++;
			}
			br.close();
		}

		return result;
	}

	private void loadFileToStaging(String localPath) throws SQLException {
		File file = new File(localPath);
		String sql = "INSERT INTO staging ( stt, maSV, hoLot, ten, ngaySinh, maLop, tenLop, sdt, email, queQuan, ghiChu) VALUES (?,?,?,?,?,?,?,?,?,?,?)";
		PreparedStatement st = conStaging.prepareStatement(sql);
//		String stt  , maSV = null, hoLot = null, ten = null, ngaySinh = null, maLop = null, tenLop = null,
//				sdt = null, email = null, queQuan = null, ghiChu = null;
		BufferedReader br = null;
		StringTokenizer stk = null;
		if (file.isDirectory()) {
			File[] f = file.listFiles();
			for (File fi : f) {
				if (fi.getPath().endsWith(".txt")) {
					try {
						br = new BufferedReader(new FileReader(fi));
						String line = br.readLine();
						if (Pattern.matches("^[0-9]*$", line.substring(0, 1))) {
							br.close();
							br = new BufferedReader(new FileReader(fi));
						}
						String dilim = "|";
						while ((line = br.readLine()) != null) {
							if (line.indexOf("\t") != -1) {
								dilim = "\t";
							}
							stk = new StringTokenizer(line, dilim);
//							while(stk.hasMoreTokens()) {
//								System.out.print(stk.nextToken()+"-");
//							}
							System.out.println();
							st.setString(1, stk.nextToken());
							st.setString(2, stk.nextToken());
							st.setString(3, stk.nextToken());
							st.setString(4, stk.nextToken());
							st.setString(5, stk.nextToken());
							st.setString(6, stk.nextToken());
							st.setString(7, stk.nextToken());
							st.setString(8, stk.nextToken());
							st.setString(9, stk.nextToken());
							st.setString(10, stk.nextToken());
							try {
								st.setString(11, stk.nextToken());
							} catch (NoSuchElementException e) {
								st.setString(11, "N/A");
							}
							st.execute();

						}
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}
		}

	}

	private void transformToWareHouse() throws SQLException {
		String sql = "SELECT * FROM staging";
		String insertSql = "INSERT INTO warehouse"
				+ "(maSV, hoLot, ten, ngaySinh, maLop, tenLop, sdt, email, queQuan, ghiChu) VALUES(?,?,?,?,?,?,?,?,?,?)";
		Statement stagingStatement = conStaging.createStatement();
		ResultSet rsStaging = stagingStatement.executeQuery(sql);
		while (rsStaging.next()) {
			PreparedStatement pStatement = conWarehouse.prepareStatement(insertSql);
			pStatement.setString(1, rsStaging.getString("maSV"));
			pStatement.setString(2, rsStaging.getString("hoLot"));
			pStatement.setString(3, rsStaging.getString("ten"));
//			pStatement.setDate(4, java.sql.Date.valueOf(rsStaging.getString("ngaySinh"))); 
			// yyyy-mm-dd
			pStatement.setString(4, rsStaging.getString("ngaySinh"));
			pStatement.setString(5, rsStaging.getString("maLop"));
			pStatement.setString(6, rsStaging.getString("tenLop"));
			pStatement.setString(7, rsStaging.getString("sdt"));
			pStatement.setString(8, rsStaging.getString("email"));
			pStatement.setString(9, rsStaging.getString("queQuan"));
			pStatement.setString(10, rsStaging.getString("ghiChu"));

			pStatement.executeUpdate();
		}
	}

	public static void main(String argv[]) throws SQLException, IOException, ClassNotFoundException {
		Warehouse cle = new Warehouse();
//		System.out.println(cle.readLine(new File("E:\\WH\\test.txt")));
		;
		cle.createConnection();
		cle.getConfig();
//		cle.downloadFile();
//		cle.wiriteLog(localPath);
		cle.loadFileToStaging("E:\\WH\\test");
		cle.transformToWareHouse();

	}
}
