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

public class RankLang extends Configured implements Tool{


    public static class MyMapper1 extends Mapper<Object, Text, Text, Text> {
	private final String Language_name = "language_used";
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
	
            String line = value.toString();
	    CSVReader R = new CSVReader(new StringReader(line));
	
	    String[] ParsedLine = R.readNext();
	    R.close();
	    String column = ParsedLine[88];
	    final String DELIMITER = ";";
            if(!column.equals("NA")){
		String[] Languages = column.split(DELIMITER);
		for(String lang : Languages){
			lang = lang.trim();
			context.write(new Text(Language_name), new Text(lang));
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
	private int top = 20;
        public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
		HashMap<String, Integer> map = new HashMap<String,Integer>();		     for (Text val : values) {
			String cur_word = val.toString();
			if(!map.containsKey(cur_word)){
				map.put(cur_word, 1);
			}else{
				map.put(cur_word, map.get(cur_word) + 1);
			}
		}
		Map<String, Integer> sortedMap = sortByValues(map);
		Set set = sortedMap.entrySet();
		Iterator iterator = set.iterator();
		String Language = "";
		String NumOfUser = "";
		while(iterator.hasNext() && top > 0){
			Map.Entry myMap = (Map.Entry)iterator.next();
			Language = myMap.getKey().toString();
			NumOfUser = myMap.getValue().toString();
			context.write(new Text(Language), new Text(NumOfUser));
			top--;
		}
        }
    }

    public static void main(String[] args) throws Exception {
	int res = ToolRunner.run(new Configuration(), new RankLang(), args);
        System.exit(res);
    }
    public int run(String[] args) throws Exception{
	Configuration conf = getConf();
        Job job = Job.getInstance(conf, "Job 1");
        job.setJarByClass(RankLang.class);
        job.setJar("RankLang.jar");
        job.setMapperClass(MyMapper1.class);
        job.setReducerClass(MyReducer1.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        return job.waitForCompletion(true) ? 0 : 1;
    }
}
