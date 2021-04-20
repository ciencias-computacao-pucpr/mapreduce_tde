package transactions.ex6;

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
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        GenericOptionsParser optionsParser = new GenericOptionsParser(conf, args);
        String[] files = optionsParser.getRemainingArgs();
        Path input = new Path(files[0]);
        Path output = new Path(files[1]);

        Job j = Job.getInstance(conf, "ex6");
        j.setJarByClass(Main.class);

        j.setMapperClass(UnitTypePricePerYearMapper.class);
        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(CommodityPrice.class);

        j.setReducerClass(UnitTypePricePerYearReducer.class);
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(Text.class);

        Paths.recursiveDeleteIfExists(output);

        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        System.exit(j.waitForCompletion(false) ? 0 : 1);
    }

    public static class UnitTypePricePerYearMapper extends Mapper <LongWritable, Text, Text, CommodityPrice>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            Transaction t = Transaction.getInstanceNoHeadersNoTotals(value.toString());
            if (t == null) return;

            context.write(new Text(String.format("%d %s", t.getYear(), t.getQuantityName())), new CommodityPrice(t.getCommodity() ,t.getTradeUsd()));
        }
    }

    private static class UnitTypePricePerYearReducer extends Reducer<Text, CommodityPrice, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<CommodityPrice> values, Context context) throws IOException, InterruptedException {
            CommodityPrice max = null;
            for (CommodityPrice com : values) {
                if (max == null || max.getValue() < com.getValue()) {
                    max = com;
                }
            }

            if (max != null)
                context.write(key, new Text(max.getComm()));
        }
    }
}
