package javademo;

import java.io.PrintStream;
import java.util.Scanner;

public class Demo1 {

	public static void main(String[] args) {
		System.out.println("out");
		System.err.println("err");
		
		Scanner scan = new Scanner(System.in);
		String s = scan.nextLine();
		System.out.println(s.length());
		
		PrintStream ps_old = System.out;
		PrintStream ps_new = null;
		try {
			ps_new = new PrintStream("D:\\test\\Demo1.log");
		} catch(Exception e) {
			System.out.println(e);
		}
		System.setOut(ps_new);
		System.out.println("log");
		System.setOut(ps_old);
		System.out.println("log print finish!");

	}

}