import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.StringTokenizer;
import java.io.StringReader;
import java.util.*;

import com.opencsv.CSVReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class AvgSalaryByCountry extends Configured implements Tool{

    private static final String M_OUTPUT_PATH = "/data/temp_out";

    public static class MyMapper1 extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
	
	    int notAva = 0;
	    String line = value.toString();
	    CSVReader R = new CSVReader(new StringReader(line));
	
	    String[] ParsedLine = R.readNext();
	    R.close();
	    String Country = ParsedLine[3].trim();
	    String Salary = ParsedLine[152].trim();
	    if(Country.equals("NA") || Salary.equals("NA") || Salary.equals("Salary")) notAva = 1;
	    if(notAva != 1){
		Double salaryInUSD = Double.valueOf(Salary);
		double t = salaryInUSD.doubleValue();
	    	context.write(new Text(Country), new Text(Double.toString(t)));
	    }
        }
    }

    private static HashMap sortByValues(HashMap map) {
	List list = new LinkedList(map.entrySet());
	Collections.sort(list, new Comparator() {
		public int compare(Object o1, Object o2) {
			 return ((Comparable) ((Map.Entry) (o2)).getValue())
				.compareTo(((Map.Entry) (o1)).getValue());
		}
	});
	
	HashMap sortedHashMap = new LinkedHashMap();
	for (Iterator it = list.iterator(); it.hasNext();) {
		Map.Entry entry = (Map.Entry) it.next();
		sortedHashMap.put(entry.getKey(), entry.getValue());
	}
	return sortedHashMap;
    }

    public static class MyReducer1 extends Reducer<Text, Text, Text, Text> {
	private int limit = 10;
        public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
		int numOfpeople = 0;
		double sum = 0.0;
		double avgSalary = 0.0;
		String AvgSalary = "";
		String Country = key.toString().replace(" ","_");
		for(Text val : values){
			sum += new Double(val.toString()).doubleValue();
			numOfpeople++;
		}
		if(numOfpeople >= limit){
			avgSalary = sum / numOfpeople;
			context.write(new Text(Country), new Text(Double.toString(avgSalary)));
		}
	}
    }

    public static class Mapper2 extends Mapper<LongWritable, Text, Text, Text> {
	private final String label = "CountryAvg";
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		StringTokenizer itr = new StringTokenizer(line);
		
		while (itr.hasMoreTokens()) {
			String country  = itr.nextToken().replace("_"," ");
			String avg = itr.nextToken();
			String out = country + "," + avg;
			context.write(new Text(label), new Text(out));
		}
	}	
    }

    public static class Reducer2 extends Reducer<Text, Text, Text, Text> {
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
	final String DELIMITER = ",";
	HashMap<String, Double> map = new HashMap<String,Double>();
	for(Text val : values){
		String[] CountryAndSal = val.toString().split(DELIMITER);
		String Country = CountryAndSal[0];
		double AvgSalary = new Double(CountryAndSal[1]).doubleValue();
	 	map.put(Country, AvgSalary);
	}
	Map<String, Double> sortedMap = sortByValues(map);
	Set set = sortedMap.entrySet();
	Iterator iterator = set.iterator();
	String CurCountry = "";
	String CurAvgSalary = "";
	while(iterator.hasNext()){
		Map.Entry myMap = (Map.Entry)iterator.next();
		CurCountry = myMap.getKey().toString();
		CurAvgSalary = myMap.getValue().toString();
		context.write(new Text(CurCountry), new Text(CurAvgSalary));
	}
}}
		
	
    public static void main(String[] args) throws Exception {
	int res = ToolRunner.run(new Configuration(), new AvgSalaryByCountry(), args);
        System.exit(res);
    }
    public int run(String[] args) throws Exception{
	Configuration conf = getConf();
        Job job = Job.getInstance(conf, "Job 1");
        job.setJarByClass(AvgSalaryByCountry.class);
        job.setJar("AvgSalaryByCountry.jar");
        job.setMapperClass(MyMapper1.class);
        job.setReducerClass(MyReducer1.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(M_OUTPUT_PATH));
	job.waitForCompletion(true);
	
	
	Job job2 = Job.getInstance(conf, "Job 2");
        job2.setJarByClass(AvgSalaryByCountry.class);
        job2.setJar("AvgSalaryByCountry.jar");
        job2.setMapperClass(Mapper2.class);
        job2.setReducerClass(Reducer2.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job2, new Path(M_OUTPUT_PATH));
        FileOutputFormat.setOutputPath(job2, new Path(args[1]));
	
        return job2.waitForCompletion(true) ? 0 : 1;
    }
}
