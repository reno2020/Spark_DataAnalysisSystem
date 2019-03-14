package cn.just.spark.test;

import java.util.HashMap;

public class Leetcode {
	 public static int lengthOfLongestSubstring(String s) {
	        if(s.length()==0){
	            return 0;
	        }
	        char[] c=s.toCharArray();
	        int maxLength=0,start=0;
	        HashMap<String,Integer> lastOccuredMap = new HashMap<String,Integer>();
	        for(int i=0;i<s.length();i++){
	            int lastIndex=(lastOccuredMap.get(String.valueOf(c[i]))!=null)?lastOccuredMap.get(String.valueOf(c[i])):-1;
	            if(lastIndex>=start){
	                start=lastIndex+1;
	            }
	            if(i-start+1>maxLength){
	                maxLength=i-start+1;
	            }
	            lastOccuredMap.put(String.valueOf(c[i]),i);
	        }
	        return maxLength;
	    }
	 
	 public static void main(String[] args) {
		System.out.println(lengthOfLongestSubstring(" "));
	}
}
