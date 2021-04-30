package transactions.ex1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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

    public static final LongWritable one = new LongWritable(1);

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        GenericOptionsParser optionsParser = new GenericOptionsParser(conf, args);
        String[] files = optionsParser.getRemainingArgs();
        Path input = new Path(files[0]);
        Path output = new Path(files[1]);
        Job j = Job.getInstance(conf, "ex1");
        j.setJarByClass(Main.class);
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        j.setMapperClass(BrazilMapper.class);
        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(LongWritable.class);

        j.setReducerClass(BrazilReducer.class);
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(LongWritable.class);

        Paths.recursiveDeleteIfExists(output);
        System.exit(j.waitForCompletion(false) ? 0 : 1);
    }

    public static class BrazilMapper extends Mapper<Object, Text, Text, LongWritable> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Transaction t = Transaction.getInstanceNoHeadersNoTotals(value.toString());

            if (t == null) return;

            if (t.getCountryOrArea().equalsIgnoreCase("brazil")) {
                context.write(new Text(t.getCountryOrArea()), one);
            }
        }
    }

    public static class BrazilReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context)
                throws IOException, InterruptedException {
            long count = 0;
            for (LongWritable v : values) {
                count += v.get();
            }
            context.write(key, new LongWritable(count));
        }
    }
}
