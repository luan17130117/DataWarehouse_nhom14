package phan1;

import java.sql.SQLException;
import java.util.Scanner;

public class Main {
	@SuppressWarnings("resource")
	public static void main(String[] args) throws ClassNotFoundException, SQLException {
		Scanner sc = new Scanner(System.in);
		System.out.println("Nhập số cần tải: ");
		int n = sc.nextInt();
		System.out.print("Bắt đầu tải: ");
		DownloadFile load = new DownloadFile();
		load.DownloadFie(n);
	}
}
