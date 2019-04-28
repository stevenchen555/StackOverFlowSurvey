import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.StringTokenizer;
import java.io.StringReader;

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

public class CsbgRatio extends Configured implements Tool{


    public static class MyMapper1 extends Mapper<Object, Text, Text, Text> {

        private Text word = new Text();
        private Text profession = new Text();
	private String Professional = "Professional developer";

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
	    CSVReader R = new CSVReader(new StringReader(line));
	
	    String[] ParsedLine = R.readNext();
	    R.close();
	    if(ParsedLine[1].equals(Professional)){
	    	profession.set(ParsedLine[1]);
	    	word.set(ParsedLine[7]);
		context.write(new Text(profession) , new Text(word));
	    }
        }
    }

    public static class MyReducer1 extends Reducer<Text, Text, Text, DoubleWritable> {

	private List<String> CSrelatedMajors = new ArrayList<String>();
	
	{
		CSrelatedMajors.add("Computer science or software engineering");
		CSrelatedMajors.add("Computer programming or Web development");
		CSrelatedMajors.add("Computer engineering or electrical/electronics engineering");
		CSrelatedMajors.add("Information technology, networking, or system administration");
	}

        public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
		
           	double total_count = 0;
		double csRelated_count = 0;
		double ratio = 0;
		String out = "CSBG ratio";
		
		for(Text val : values){
			total_count++;
			if(CSrelatedMajors.contains(val.toString())){
				csRelated_count++;
			}
		} 
		ratio = csRelated_count / total_count;

		context.write(new Text(out), new DoubleWritable(ratio));
        }
    }

    public static void main(String[] args) throws Exception {
	int res = ToolRunner.run(new Configuration(), new CsbgRatio(), args);
        System.exit(res);
    }
    public int run(String[] args) throws Exception{
	Configuration conf = getConf();
        Job job = Job.getInstance(conf, "Job 1");
        job.setJarByClass(CsbgRatio.class);
        job.setJar("CsbgRatio.jar");
        job.setMapperClass(MyMapper1.class);
        job.setReducerClass(MyReducer1.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        return job.waitForCompletion(true) ? 0 : 1;
    }
}
