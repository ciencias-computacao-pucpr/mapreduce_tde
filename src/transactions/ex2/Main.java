package transactions.ex2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import util.JobBuilder;
import util.Transaction;

import java.io.IOException;

public class Main {

    public static final LongWritable one = new LongWritable(1);
    public static final Text brazil = new Text("Brazil");
    public static final String header = "country_or_area;year;comm_code;commodity;flow;trade_usd;weight_kg;quantity_name;quantity;category";

    public static void main(String[] args) throws Exception {

        Job job = new JobBuilder(Main.class, args)
                .deleteOutputIfExists()
                .name("transactions.peryear")
                .mapperClass(PerYearTransaction.class, IntWritable.class, LongWritable.class)
                .reducerClass(PerYearReducer.class, IntWritable.class, LongWritable.class)
                .build();

        System.exit(job.waitForCompletion(false) ? 0 : 1);
    }

    public static class PerYearTransaction extends Mapper<Object, Text, IntWritable, LongWritable> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            if (!header.equals(value.toString())) {
                final Transaction transaction = new Transaction(value.toString());
                context.write(new IntWritable(transaction.getYear()), one);
            }
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
