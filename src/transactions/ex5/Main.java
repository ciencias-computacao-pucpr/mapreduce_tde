package transactions.ex5;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.examples.pi.math.Bellard;
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
import transactions.ex4.SumCount;
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

        Job j = Job.getInstance(conf, "ex5");
        j.setJarByClass(Main.class);

        j.setMapperClass(UnitTypeYearCategoryMapper.class);
        j.setMapOutputKeyClass(UnitTypeYearCategory.class);
        j.setMapOutputValueClass(SumCount.class);

        j.setCombinerClass(UnitTypeYearCategoryCombiner.class);

        j.setReducerClass(UnitTypeYearCategoryReducer.class);
        j.setOutputKeyClass(UnitTypeYearCategory.class);
        j.setOutputValueClass(DoubleWritable.class);

        Paths.recursiveDeleteIfExists(output);

        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        System.exit(j.waitForCompletion(false) ? 0 : 1);
    }

    public static class UnitTypeYearCategoryMapper extends Mapper<LongWritable, Text, UnitTypeYearCategory, SumCount> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            Transaction t = Transaction.getInstanceNoHeadersNoTotals(value.toString());

            if (t == null) return;

            if (t.getFlow().equalsIgnoreCase("EXPORT") && t.getCountryOrArea().equalsIgnoreCase("BRAZIL")) {
                context.write(new UnitTypeYearCategory(t.getQuantityName(), t.getYear(), t.getCategory()), new SumCount(t.getTradeUsd(), 1));
            }
        }
    }

    public static class UnitTypeYearCategoryCombiner extends Reducer<UnitTypeYearCategory, SumCount, UnitTypeYearCategory, SumCount> {
        @Override
        protected void reduce(UnitTypeYearCategory key, Iterable<SumCount> values, Context context) throws IOException, InterruptedException {
            SumCount total = new SumCount();
            values.forEach(v ->  {
                double totalValue = total.getTotal() + v.getTotal();
                long totalCount = total.getCount() + v.getCount();
                total.setTotal(totalValue / totalCount);
                total.setCount(totalCount);
            });

            context.write(key, total);
        }
    }

    public static class UnitTypeYearCategoryReducer extends Reducer<UnitTypeYearCategory, SumCount, UnitTypeYearCategory, DoubleWritable> {
        @Override
        protected void reduce(UnitTypeYearCategory key, Iterable<SumCount> values, Context context) throws IOException, InterruptedException {
            SumCount total = new SumCount();
            values.forEach(v ->  {
                double totalValue = total.getTotal() + v.getTotal();
                long totalCount = total.getCount() + v.getCount();
                total.setTotal(totalValue);
                total.setCount(totalCount);
            });


            context.write(key, new DoubleWritable(total.getTotal() / total.getCount()));
        }
    }
}
