package main;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Scanner;

import connection.database.GetConnection;
import phan1.DownloadFile;
import phan2.Staging;

public class Main {
	public Main() {

	}

	public static void main(String[] args) throws ClassNotFoundException, SQLException {
		while (true) {
			System.out.println("Nhập số tùy chọn:\n 1. Run\n 0. Exit");
			Scanner sc = new Scanner(System.in);
			int value = sc.nextInt();
			if (value == 1) {
				//Download
				System.out.println("Nhap so can tai: ");
				int n = sc.nextInt();
				System.out.print("Bắt đầu tải: ");
				DownloadFile load = new DownloadFile();
				load.DownloadFie(n);
				// staging
				new Staging().loadStudentToStaging();
			} else if (value == 0) {
				System.out.println("Hẹn gặp lại sau!");
				break;
			} else {
				System.out.println("Nhập không đúng số trong danh mục!");
			}

		}
	}

}
