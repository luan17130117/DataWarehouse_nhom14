package phan1;

import java.sql.SQLException;
import java.util.Scanner;

public class Main {
	@SuppressWarnings("resource")
	public static void main(String[] args) throws ClassNotFoundException, SQLException {
		System.out.print("Nhập key cần tải:");
		Scanner sc = new Scanner(System.in);
		int n = sc.nextInt();
		System.out.print("Bắt đầu tải: ");
		DownloadFile load = new DownloadFile();
		load.DownloadFie(n);
	}
}
