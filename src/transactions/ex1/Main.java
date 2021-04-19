package transactions.ex1;

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

        Job job = new JobBuilder(Main.class ,args)
                .deleteOutputIfExists()
                .name("transactions.brazil")
                .mapperClass(BrazilMapper.class, Text.class, LongWritable.class)
                .reducerClass(BrazilReducer.class, Text.class, LongWritable.class)
                .build();

        System.exit(job.waitForCompletion(false) ? 0 : 1);
    }

    public static class BrazilMapper extends Mapper<Object, Text, Text, LongWritable> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            if (!header.equals(value.toString())) {
                final Transaction transaction = new Transaction(value.toString());
                context.write(new Text(transaction.getCountryOrArea()), one);
            }
        }
    }

    public static class BrazilReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            if (key.equals(brazil)) {
                long count = 0;
                for (LongWritable v : values) {
                    count += v.get();
                }
                context.write(key, new LongWritable(count));
            }
        }
    }
}
