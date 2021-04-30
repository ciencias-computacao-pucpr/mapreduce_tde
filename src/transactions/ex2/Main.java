package transactions.ex2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import util.Paths;
import util.Transaction;

import java.io.IOException;

public class Main {

    public static LongWritable one = new LongWritable(1);

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        GenericOptionsParser optionsParser = new GenericOptionsParser(conf, args);
        String[] files = optionsParser.getRemainingArgs();
        Path input = new Path(files[0]);
        Path output = new Path(files[1]);
        Job j = Job.getInstance(conf, "ex2");
        j.setJarByClass(Main.class);
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        j.setMapperClass(PerYearTransactionMapper.class);
        j.setMapOutputKeyClass(IntWritable.class);
        j.setMapOutputValueClass(LongWritable.class);

        j.setReducerClass(PerYearReducer.class);
        j.setOutputKeyClass(IntWritable.class);
        j.setOutputValueClass(LongWritable.class);

        Paths.recursiveDeleteIfExists(output);
        System.exit(j.waitForCompletion(false) ? 0 : 1);
    }

    public static class PerYearTransactionMapper extends Mapper<Object, Text, IntWritable, LongWritable> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Transaction t = Transaction.getInstanceNoHeadersNoTotals(value.toString());
            if (t == null) return;

            context.write(new IntWritable(t.getYear()), one);
        }
    }

    public static class PerYearReducer extends Reducer<IntWritable, LongWritable, IntWritable, LongWritable> {
        @Override
        protected void reduce(IntWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long count = 0;
            for (LongWritable v : values) {
                count += v.get();
            }
            context.write(key, new LongWritable(count));
        }
    }
}
