package transactions.ex3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import transactions.ex3.beans.CommAmount;
import transactions.ex3.beans.CommFlowType;
import util.Paths;
import util.Transaction;

import java.io.IOException;

public class Main {

    public static final LongWritable one = new LongWritable(1);
    public static final String header = "country_or_area;year;comm_code;commodity;flow;trade_usd;weight_kg;quantity_name;quantity;category";

    public static void main(String[] args) throws Exception {
        final Configuration conf = new Configuration();
        final GenericOptionsParser genericOptionsParser = new GenericOptionsParser(conf, args);
        final Job j1 = Job.getInstance(conf, "2016_pt1");
        j1.setJarByClass(Main.class);

        final String[] files = genericOptionsParser.getRemainingArgs();
        Path inputFile = new Path(files[0]);
        Path outputDir = new Path(files[1]);
        Path intermediate = new Path(files[1] + "_intermediate");

        FileInputFormat.addInputPath(j1, inputFile);
        FileOutputFormat.setOutputPath(j1, intermediate);

        j1.setMapperClass(CommodityIn2016.class);
        j1.setMapOutputKeyClass(CommFlowType.class);
        j1.setMapOutputValueClass(DoubleWritable.class);

        j1.setReducerClass(CommodityIn2016Reducer.class);
        j1.setOutputKeyClass(CommFlowType.class);
        j1.setMapOutputValueClass(DoubleWritable.class);

        Paths.recursiveDeleteIfExists(intermediate);
        if (!j1.waitForCompletion(false)) {
            System.out.println("Primeira parte falhou");
            System.exit(1);
        }

        Job j2 = Job.getInstance(conf, "2016_pt2");
        j2.setJarByClass(Main.class);

        j2.setMapperClass(MostIn2016Mapper.class);
        j2.setMapOutputKeyClass(Text.class);
        j2.setMapOutputValueClass(CommAmount.class);

        j2.setReducerClass(MostIn2016Reducer.class);
        j2.setOutputKeyClass(Text.class);
        j2.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(j2, intermediate);
        FileOutputFormat.setOutputPath(j2, outputDir);

        Paths.recursiveDeleteIfExists(outputDir);

        System.exit(j2.waitForCompletion(false) ? 0 : 1);
    }

    public static class MostIn2016Mapper extends Mapper<Object, Text, Text, CommAmount> {
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            System.out.println(key);
            String[] split = value.toString().split("\t");
            String comm = split[0];
            String flow = split[1];
            Double amount = Double.parseDouble(split[2]);
            context.write(new Text(flow), new CommAmount(comm, amount));
        }
    }

    public static class MostIn2016Reducer extends Reducer<Text, CommAmount, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<CommAmount> values, Context context) throws IOException, InterruptedException {
            CommAmount max = null;
            for (CommAmount ca : values) {
                if (max == null || ca.getAmount() > max.getAmount()) {
                    max = ca;
                }
            }
            if (max != null)
                context.write(key, new Text(max.getComm()));
        }
    }

    public static class CommodityIn2016 extends Mapper<Object, Text, CommFlowType, DoubleWritable> {
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Transaction t = Transaction.getInstanceNoHeadersNoTotals(value.toString());

            if (t == null) return;

            context.write(new CommFlowType(t.getCommodity(), t.getFlow()), new DoubleWritable(t.getWeightKg()));
        }
    }

    public static class CommodityIn2016Reducer extends Reducer<CommFlowType, DoubleWritable, CommFlowType, DoubleWritable> {
        @Override
        protected void reduce(CommFlowType key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double sum = 0.0;
            for (DoubleWritable value : values) {
                sum += value.get();
            }

            context.write(key, new DoubleWritable(sum));
        }
    }

}
