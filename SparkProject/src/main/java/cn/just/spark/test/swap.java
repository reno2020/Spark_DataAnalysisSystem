package cn.just.spark.test;

import java.util.HashMap;

public class swap {
	public int age;
	public swap(int age) {
		this.age=age;
	}
	public static void swaped(swap s1,swap s2) {
		swap tmp;
		tmp=s1;
		s1=s2;
		s2=tmp;
		System.out.println(s1.age+"======="+s2.age);
		System.out.println(s1.hashCode()+"====="+s2.hashCode());
	}
	public static void main(String[] args) {
		swap s1=new swap(20);
		swap s2=new swap(40);
		System.out.println(s1.age+"======="+s2.age);
		System.out.println(s1.hashCode()+"====="+s2.hashCode());
		swaped(s1,s2);
		System.out.println(s1.age+"======="+s2.age);
		System.out.println(s1.hashCode()+"====="+s2.hashCode());
		double a=1.22547,b=1.22547;
		System.out.println(a==b);
		
//		String s=" ";
//		char[] c=s.toCharArray();
//		System.out.println(String.valueOf(c).length());
//		HashMap<String,Integer> map = new HashMap<String,Integer>();
//		map.put(" ", 1);
//		System.out.println(map.get(" "));
//		System.out.println(1&1);
		System.out.println(4^5^4^5^3^2^3^4);
		System.out.println(4&4&4&3&4);
	}
}
