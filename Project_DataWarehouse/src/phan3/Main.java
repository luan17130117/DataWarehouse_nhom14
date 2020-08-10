package phan3;

import java.util.Scanner;

import phan2.Staging;

public class Main {
	public static void main(String[] args) {
		new Main().loadToWH();
	}
	public void loadToWH() {
		Scanner sc = new Scanner(System.in);
		System.out.print(
				"Nhap so can load to DW:\n 1. load sinhvien to DW \n 2. load monhoc to DW \n 3. load lophoc to DW \n 4. load dangky to DW \n 0. Exit \n ");
		int n = sc.nextInt();
		if (n == 1) {
			new DataWarehouseStudent().insertStudentToDW();
		} else if (n == 2) {
			new DataWarehouseSubject().insertSubjectToDW();
		} else if (n == 3) {
			new DataWarehouseClass().insertClassToDW();
		} else if (n == 4) {
			new DataWarehouseRegistration().insertRegistrationToDW();
		} else if (n == 0) {
			System.out.println("See you again!");
		} else {
			System.out.println("Nhap khong dung so trong danh muc! Moi ban nhap lai!");
			new Main().loadToWH();
		}
	}
}
