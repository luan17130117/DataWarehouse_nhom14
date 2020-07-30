package main;

import java.util.*;

public class MyTask extends TimerTask {

	public void run() {
		System.out.println("timer working");
	}

	public static void main(String[] args) {

		// TimerTask là công việc được thực
		MyTask myTask = new MyTask();

		// Timer là lịch trình thực thi.
		Timer timer = new Timer();

		/*
		 * myTask : task được thực thi 
		 * 100: thời gian chờ chạy lần đầu 
		 * 2000: sau 2s thì chạy cái tiếp theo
		 */
		timer.schedule(myTask, 100, 2000);
	}

}