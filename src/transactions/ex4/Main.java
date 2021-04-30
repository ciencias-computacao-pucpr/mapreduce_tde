package transactions.ex4;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        GenericOptionsParser optionsParser = new GenericOptionsParser(conf, args);
        String[] files = optionsParser.getRemainingArgs();
        Path input = new Path(files[0]);
        Path output = new Path(files[1]);
        Job j = Job.getInstance(conf, "avg_comm_values");
        j.setJarByClass(Main.class);
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        j.setJarByClass(Main.class);
        j.setMapperClass(CommMapper.class);
        j.setMapOutputKeyClass(IntWritable.class);
        j.setMapOutputValueClass(SumCount.class);

        j.setCombinerClass(CommCombiner.class);

        j.setReducerClass(CommReducer.class);
        j.setOutputValueClass(IntWritable.class);
        j.setOutputValueClass(Text.class);

        Paths.recursiveDeleteIfExists(output);
        System.exit(j.waitForCompletion(false) ? 0 : 1);
    }

    public static class CommMapper extends Mapper<LongWritable, Text, IntWritable, SumCount> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            Transaction t = Transaction.getInstanceNoHeadersNoTotals(value.toString());
            if (t != null) {
                context.write(new IntWritable(t.getYear()), new SumCount(t.getTradeUsd(), 1));
            }
        }
    }

    public static class CommCombiner extends Reducer<IntWritable, SumCount, IntWritable, SumCount> {
        @Override
        protected void reduce(IntWritable key, Iterable<SumCount> values, Context context) throws IOException, InterruptedException {
            SumCount total = new SumCount(0.0,0);
            for (SumCount value : values) {
                total.setCount(total.getCount() + value.getCount());
                total.setTotal(total.getTotal() + value.getTotal());
            }

            context.write(key, total);
        }
    }

    public static class CommReducer extends Reducer<IntWritable, SumCount, IntWritable, Text> {
        @Override
        protected void reduce(IntWritable key, Iterable<SumCount> values, Context context) throws IOException, InterruptedException {
            SumCount total = new SumCount(0.0,0);
            for (SumCount value : values) {
                total.setTotal(total.getTotal() + value.getTotal());
                total.setCount(total.getCount() + value.getCount());
            }

            context.write(key, new Text(String.format("%.2f", total.getTotal() / total.getCount())));
        }
    }
}
