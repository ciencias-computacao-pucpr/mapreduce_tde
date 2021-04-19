package util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

public class JobUtils {

    public static final String TXS_FILE_HEADER = "country_or_area;year;comm_code;commodity;flow;trade_usd;weight_kg;quantity_name;quantity;category";

    public static Job parseOptionsAndPaths(Class<?> jarClass, String[] args) throws IOException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(jarClass);
        final GenericOptionsParser genericOptionsParser = new GenericOptionsParser(conf, args);
        String[] files = genericOptionsParser.getRemainingArgs();
        Path inputFile = new Path(files[0]);
        Path outputDir = new Path(files[1]);

        FileInputFormat.addInputPath(job, inputFile);
        FileOutputFormat.setOutputPath(job, outputDir);

        Paths.recursiveDeleteIfExists(outputDir);

        return job;
    }

    public static <KEYOUT, VALUEOUT> void configMapper(Job j, Class<? extends Mapper<Object, Text, KEYOUT, VALUEOUT>> mapper, Class<KEYOUT> keyoutput, Class<VALUEOUT> valueoutput) {
        j.setMapperClass(mapper);
        j.setMapOutputKeyClass(keyoutput);
        j.setMapOutputValueClass(valueoutput);
    }

    public static <MKEYOUT, MVALUEOUT, RKEYOUT, RVALUEOUT> void configReducer(
            Job j,
            Class<? extends Mapper<Object, Text, MKEYOUT, MVALUEOUT>> mapperClass,
            Class<? extends Reducer<MKEYOUT, MVALUEOUT, RKEYOUT, RVALUEOUT>> reducerClass,
            Class<RKEYOUT> reducerKeyOutputClass,
            Class<RVALUEOUT> reducerValueOutputClass) {

        j.setReducerClass(reducerClass);
        j.setOutputKeyClass(reducerKeyOutputClass);
        j.setOutputValueClass(reducerValueOutputClass);
    }



    public static <MKEYOUT, MVALUEOUT, CKEYOUT, CVALUEOUT, RKEYOUT, RVALUEOUT> void configCombinerReducer(
            Job j,
            Class<? extends Mapper<Object, Text, MKEYOUT, MVALUEOUT>> mapperClass,
            Class<? extends Reducer<MKEYOUT, MVALUEOUT, CKEYOUT, CVALUEOUT>> combinerClass,
            Class<? extends Reducer<CKEYOUT, CVALUEOUT, RKEYOUT, RVALUEOUT>> reducerClass,
            Class<RKEYOUT> reducerKeyOutputClass,
            Class<RVALUEOUT> reducerValueOutputClass) {

        j.setCombinerClass(combinerClass);

        j.setReducerClass(reducerClass);
        j.setOutputKeyClass(reducerKeyOutputClass);
        j.setOutputValueClass(reducerValueOutputClass);
    }
}
