import java.io.IOException;
import java.io.StringReader;

import com.opencsv.CSVReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.apache.hadoop.conf.Configured;

public class avgSalaryByCountry extends Configured implements Tool {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, DoubleWritable> {
        private Text country = new Text();
        private DoubleWritable salary = new DoubleWritable();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            CSVReader R = new CSVReader(new StringReader(line));
            String na = "NA";
            String[] ParsedLine = R.readNext();
            R.close();
            if (!ParsedLine[152].equals(na) && !ParsedLine[152].equals("Salary")) {
                country.set(ParsedLine[3]);
                Double test = Double.valueOf(ParsedLine[152]);
                salary.set(test);
                context.write(country, salary);
            }
        }
    }

    public static class avgSalary extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        private DoubleWritable salary = new DoubleWritable();

        public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {
            int base = 0;
            double sum = 0;
            for (DoubleWritable val : values) {
                base++;
                sum += val.get();
            }
            sum /= base;
            salary.set(sum);
            System.out.println(key.toString());
            System.out.println(salary.get());
            context.write(key, salary);
        }
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new avgSalaryByCountry(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Job 1");
        job.setJarByClass(avgSalaryByCountry.class);
        job.setJar("avgSalaryByCountry.jar");
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(avgSalary.class);
        job.setReducerClass(avgSalary.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        return job.waitForCompletion(true) ? 0 : 1;
    }
}