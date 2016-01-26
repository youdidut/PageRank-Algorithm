import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.IOException;
import java.util.Iterator;


import org.apache.hadoop.mapred.MapReduceBase;  
import org.apache.hadoop.mapred.OutputCollector;  
import org.apache.hadoop.mapred.Reducer;  
import org.apache.hadoop.mapred.Reporter;  
import org.apache.hadoop.io.LongWritable;   
import org.apache.hadoop.mapred.MapReduceBase;  
import org.apache.hadoop.mapred.Mapper;  
import org.apache.hadoop.mapred.OutputCollector;    
import org.apache.hadoop.mapred.Reporter; 
import org.apache.hadoop.fs.Path;  
import org.apache.hadoop.io.Text;  
import org.apache.hadoop.mapred.FileInputFormat;  
import org.apache.hadoop.mapred.FileOutputFormat;  
import org.apache.hadoop.mapred.JobClient;  
import org.apache.hadoop.mapred.JobConf; 
import org.apache.hadoop.mapred.TextInputFormat;  
import org.apache.hadoop.mapred.TextOutputFormat; 


public class PageRank {	
	static String original;
	static String input="input";
	static String output="output";
    	static int nodeNum=0;
    	static int edgeNum=0;
	static int minDegree=Integer.MAX_VALUE;
	static int maxDegree=0;
	static double aveDegree=0;
	static int i=0;
        static String convRate="0.01";
	static String firstTen="0";
	
	public static void main(String[] args) throws Exception {
		original=args[1];	
	    	if(args[0].equals("first")){
			createFirst();
			return;
	        }
	        	
		createFirst();		
		convRate=args[2];
		long beginTime = System.currentTimeMillis();
		while(true){
		    JobConf conf = new JobConf(PageRank.class);  
		    conf.set("nodeNum",Integer.toString(nodeNum));   
		    conf.set("i",Integer.toString(i));
		    conf.set("convRate",convRate);
		    conf.set("firstTen",firstTen);
		    conf.set("beginTime",Long.toString(beginTime));
		    conf.setOutputKeyClass(Text.class);  
		    conf.setOutputValueClass(Text.class);  
		      
		    conf.setInputFormat(TextInputFormat.class);  
		    conf.setOutputFormat(TextOutputFormat.class);  
		    if(i==0){
		    	FileInputFormat.setInputPaths(conf, new Path("in/"+output+Integer.toString(i)));  
		    	FileOutputFormat.setOutputPath(conf, new Path("in/"+output+Integer.toString(i+1)));  
		    }
		    else{
			FileInputFormat.setInputPaths(conf, new Path("in/"+output+Integer.toString(i)+"/part-00000"));  
		    	FileOutputFormat.setOutputPath(conf, new Path("in/"+output+Integer.toString(i+1)));
		     }  
		    conf.setMapperClass(PageMapper.class);  
		    conf.setReducerClass(PageReducer.class);  
		      
		    JobClient.runJob(conf);
		    if(i==9){
			long endTime10 = System.currentTimeMillis()-beginTime;
			firstTen=Long.toString((endTime10/1000/60));
		    }
		    i++;
			            	    			
	       }
		

	}

	static void createFirst(){
		try{
			String encoding="GBK";
	        	File file=new File(original);		
			if(file.isFile() && file.exists()){                           //if file exits
	                    InputStreamReader read = new InputStreamReader(new FileInputStream(file),encoding);
	                    BufferedReader bufferedReader = new BufferedReader(read);
	                    String lineTxt = null;
	                    while((lineTxt=bufferedReader.readLine())!= null){	
	                    	nodeNum++;
				String[] sSet=lineTxt.split(" ");
				edgeNum+=sSet.length-1;
				maxDegree=Math.max(maxDegree,sSet.length-1);
				minDegree=Math.min(minDegree,sSet.length-1);
 			    }
			    aveDegree=(double)edgeNum/nodeNum;
	                    read.close();
			    System.out.println("======================================");
    			    System.out.println("=  Num nodes:          " + nodeNum);
    			    System.out.println("=  Num egdges:         " + edgeNum);
    			    System.out.println("=  Min degree:         " + minDegree);
			    System.out.println("=  Max degree:         " + maxDegree);
			    System.out.println("=  Avg degree:         " + aveDegree);
    		            System.out.println("======================================");
	        }
			else
	            System.out.println("Cannot find input file");
		}
		catch (Exception e) {
		    System.out.println("Error encounted when reading/writting from file");
		    e.printStackTrace();
		}

		
		try{
			String encoding="GBK";
	        	File file=new File(original);	
			double initial=(double)1/nodeNum;	
			if(file.isFile() && file.exists()){                           //if file exits
	                    InputStreamReader read = new InputStreamReader(new FileInputStream(file),encoding);
	                    BufferedReader bufferedReader = new BufferedReader(read);	     
	                    String lineTxt = null;
	                    PrintWriter writer = new PrintWriter("in/"+output+"0", "UTF-8");
	                    while((lineTxt=bufferedReader.readLine())!= null){	                    	                    	
	                    	lineTxt=lineTxt+" "+Double.toString(initial)+" 0";
				lineTxt=lineTxt.replaceFirst(" ","\t");	
	                    	writer.println(lineTxt);	
	                    }
	                    read.close();
	                    writer.close();
	        }
			else
	            System.out.println("Cannot find input file");
		}
        catch (Exception e) {
            System.out.println("Error encounted when reading/writting from file");
            e.printStackTrace();
        }
	}


//////////////////////////////////////////////////////////////////////////////
public static class PageReducer  extends MapReduceBase implements Reducer<Text, Text, Text, Text>{
	private static String firstTen;
        private Double pr;  
        private static int nodeNum,i;
	private static double convRate;
	private static Long beginTime;
	public void configure(JobConf job) {
    		nodeNum = Integer.parseInt(job.get("nodeNum"));
		i = Integer.parseInt(job.get("i"));
		convRate = Double.parseDouble(job.get("convRate"));
		beginTime = Long.parseLong(job.get("beginTime"));
		firstTen = job.get("firstTen");
	}
         public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> out, Reporter reporter){  
            double sum = 0;  
            String url = "";
	    String prepr="";  
 	    String converge="";
	    boolean finish=true;
	    if(!key.toString().equals("converge")){
 		while(values.hasNext()) {                   
		        String dw = values.next().toString();  		    
			if(dw.contains("~")){
			    prepr=dw.replace("~","");
			}                
			else if(!dw.contains("*")){  
		            sum += Double.valueOf(dw);  
		        }
			else{  
		            url= dw.replace("*", "");  
		        }  
		          
		    }  
		    
		    pr=0.85*sum+0.15*(double)1/nodeNum;  
		    try {  
		        out.collect(key, new Text(url+pr+" "+prepr+""));  
		    } catch (IOException e) {  
		        e.printStackTrace();  
		    } 
		}
	    else {
		    while(values.hasNext()) {                   
		        converge = values.next().toString();  
			finish=finish&&(converge.equals("true")?true:false);
			if(finish){	
			     long endTime = System.currentTimeMillis()-beginTime;
			     System.out.println("======================================");	
			     System.out.println("The first 10 runs takes:    "+firstTen+" minutes");		     
			     System.out.println("Total execution times:      "+Integer.toString(i));
		             System.out.println("Convergence below:          " + Double.toString((double)convRate/nodeNum));
			     System.out.println("Total time taken:           " + Long.toString(endTime/1000)+" s" +",which is about, "+Long.toString(endTime/1000/60) + " minutes");	
			     System.out.println("Average time per run:       " + Long.toString(endTime/i/1000)+" s/time");	
			     System.out.println("======================================");
			     System.exit(0);
			}
		    }
	          }	
        }  
    }  
///////////////////////////////////////////////////////////////////////////////////////////////
public static class PageMapper extends MapReduceBase implements  
Mapper<LongWritable, Text, Text, Text>{  
	Text id;  
    	boolean Converge=true; 
        private static int nodeNum,i;
	private static double convRate;
	public void configure(JobConf job) {
    		nodeNum = Integer.parseInt(job.get("nodeNum"));
		i = Integer.parseInt(job.get("i"));
		convRate = Double.parseDouble(job.get("convRate"));
	}

    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {           
        String line = value.toString();  
        String[] values = line.split("\t");         
        String s=values[1];
	System.out.println(line);
        String[] rest=s.split(" ");
	if(Math.abs(Double.parseDouble(rest[rest.length-1])-Double.parseDouble(rest[rest.length-2]))>(double)convRate/nodeNum)
		Converge=Converge&&false;
	System.out.println("converge "+Double.toString(Math.abs(Double.parseDouble(rest[rest.length-1])-Double.parseDouble(rest[rest.length-2]))));
	System.out.println(Converge);
        String url="*";  
	String prepr="~";
        double pr= Double.parseDouble(rest[rest.length-2]);
 	prepr=prepr+rest[rest.length-2];
	output.collect(new Text(values[0]), new Text(prepr));

	if(rest.length>2){
        for (int i = 0; i < rest.length-2; i++) {  
	    if(rest[i].equals(""))
		continue;	
            url+=rest[i]+" ";  
            id = new Text(rest[i]);
	    
	    //System.out.println("mapper:"+rest[i]+ "---"+String.valueOf(pr/(rest.length-2)));             
            output.collect(id, new Text(String.valueOf(pr/(rest.length-2))+""));  
        }  
        }  
       	output.collect(new Text("converge"), new Text(Converge?"true":"false")); 
        output.collect(new Text(values[0]), new Text(url));  
    }  

}  



	
}

