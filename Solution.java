import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.TreeMap;

public class Solution {
	public class pair{	
		String name;
		Double PR;
		
		pair(String s, Double d){
			this.name=s;
			this.PR=d;
		}
	}
	
	void getTopTen(){
		try{
			String encoding="GBK";
	        	File file=new File("C:\\Users\\Di\\Documents\\output1");		
			if(file.isFile() && file.exists()){                           //if file exits
	            ArrayList<pair> p=new ArrayList<pair>();     
				InputStreamReader read = new InputStreamReader(new FileInputStream(file),encoding);
	            BufferedReader bufferedReader = new BufferedReader(read);
	            String lineTxt = null;
	            while((lineTxt=bufferedReader.readLine())!= null){	
	                 	String[] s=lineTxt.split("\t");
	                 	String name=s[0];
	                 	String[] s1=s[1].split(" ");
						p.add(new pair(name,Double.parseDouble(s1[s1.length-2])));
	            }
	            order o=new order();
			    Collections.sort(p,o);
			    for(int i=0;i<10;i++)
			    	System.out.println(Integer.toString(i+1)+". "+p.get(i).name+": "+Double.toString(p.get(i).PR));
	        }
			else
	            System.out.println("Cannot find input file");
		}
		catch (Exception e) {
		    System.out.println("Error encounted when reading/writting from file");
		    e.printStackTrace();
		}
	}
	
	static class order implements Comparator<pair> {    	 
		public int compare(pair p1, pair p2) {
			return (p2.PR - p1.PR)>0?1:-1;
		}
	}
	
}

