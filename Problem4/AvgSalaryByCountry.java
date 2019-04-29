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

    private static Map<String, Double> createMap(){
	Map<String, Double> myMap = new HashMap<String, Double>();
	myMap.put("U.S. dollars", 1.0);
	myMap.put("Euros", 1.12);
	myMap.put("British pounds sterling", 1.29);
	myMap.put("Japanese yen", 0.009);
	myMap.put("Chinese yuan renminbi", 0.15);
	myMap.put("Brazilian reais", 0.25);
	myMap.put("Indian rupees", 0.014);
	myMap.put("Mexican pesos", 0.053);
	myMap.put("South African rands", 0.07);
	myMap.put("Swedish kroner", 0.11);
	myMap.put("Australian dollars", 0.7);
	myMap.put("Canadian dollars", 0.74);
	myMap.put("Singapore dollars", 0.73);
	myMap.put("Russian rubles", 0.015);
	myMap.put("Swiss franc", 0.98);
	myMap.put("Polish złoty", 0.26);
	myMap.put("Bitcoin", 5141.55);
	return myMap;
    }

    private static double parseSalary(String buf){
	final String DELIMITER = "e";
	if(buf.contains(DELIMITER)){
		String[] exp = buf.split(DELIMITER);
		if (exp.length > 2) return -1;
		double base = new Double(exp[0]).doubleValue(); 	
		int level = Integer.parseInt(exp[1]);
		return base*Math.pow(10, level);
	}else{
		return new Double(buf).doubleValue();
	}
    }

    public static class MyMapper1 extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
	
            int find = 0;
	    int notAva = 0;
	    String line = value.toString();
	    CSVReader R = new CSVReader(new StringReader(line));
	
	    String[] ParsedLine = R.readNext();
	    R.close();
	    String Country = ParsedLine[3].trim();
	    String Currency_type = ParsedLine[79].trim();
	    String Salary = ParsedLine[152].trim();
	    if(Country.equals("NA") || Currency_type.equals("NA") || Salary.equals("NA")) notAva = 1;
	    if(notAva != 1){
	    	Map<String, Double> CurrMap = createMap();
	    	Set set = CurrMap.entrySet();
            	Iterator iterator = set.iterator();
            	String pattern = "";
	    	double ratio = 0.0;
	    	double salaryInUSD = 0.0;
            	while(iterator.hasNext()){
	    		Map.Entry myMap = (Map.Entry)iterator.next();
                	pattern = myMap.getKey().toString();
                	if(Currency_type.contains(pattern)){
				find = 1;
				ratio = CurrMap.get(pattern);
				break;
			}	
	    	}
	    	if(find > 0){
			salaryInUSD = ratio*parseSalary(Salary);
	    		context.write(new Text(Country), new Text(Double.toString(salaryInUSD)));
            	}
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
