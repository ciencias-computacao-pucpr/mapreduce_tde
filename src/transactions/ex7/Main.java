package transactions.ex7;

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
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        GenericOptionsParser optionsParser = new GenericOptionsParser(conf, args);
        String[] files = optionsParser.getRemainingArgs();
        Path input = new Path(files[0]);
        Path output = new Path(files[1]);

        Job j = Job.getInstance(conf, "ex7");
        j.setJarByClass(Main.class);

        j.setMapperClass(YearFlowMapper.class);
        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(IntWritable.class);

        j.setCombinerClass(YearFlowReducer.class);

        j.setReducerClass(YearFlowReducer.class);
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(IntWritable.class);

        Paths.recursiveDeleteIfExists(output);

        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        System.exit(j.waitForCompletion(false) ? 0 : 1);
    }

    public static class YearFlowMapper extends Mapper <LongWritable, Text, Text, IntWritable>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            Transaction t = Transaction.getInstanceNoHeadersNoTotals(value.toString());
            if (t == null) return;

            context.write(new Text(String.format("%d %s", t.getYear(), t.getFlow())), new IntWritable(1));
        }
    }

    private static class YearFlowReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int total = 0;
            for (IntWritable qtd : values) {
                total += qtd.get();
            }

            context.write(key, new IntWritable(total));
        }
    }
}
