package util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

public class JobBuilder {

    private final Configuration conf;
    private final Job job;
    private boolean deleteOutIfExists = false;
    private Path outputDir;

    public JobBuilder(Class<?> jarClass, String[] args) throws IOException {
        this.conf = new Configuration();
        this.job = parseOptionsAndPaths(jarClass, args);
    }

    public JobBuilder deleteOutputIfExists() {
        this.deleteOutIfExists = true;
        return this;
    }

    private Job parseOptionsAndPaths(Class<?> jarClass, String[] args) throws IOException {
        Job job = Job.getInstance(conf);
        job.setJarByClass(jarClass);
        final GenericOptionsParser genericOptionsParser = new GenericOptionsParser(conf, args);
        String[] files = genericOptionsParser.getRemainingArgs();
        Path inputFile = new Path(files[0]);
        outputDir = new Path(files[1]);

        FileInputFormat.addInputPath(job, inputFile);
        FileOutputFormat.setOutputPath(job, outputDir);

        return job;
    }

    public JobBuilder name(String name) {
        this.job.setJobName(name);
        return this;
    }

    public <KEY extends Writable, VALUE extends Writable> JobBuilder mapperClass(Class<? extends Mapper<?, Text, KEY, VALUE>> cls, Class<KEY> mapKeyClass, Class<VALUE> mapOutputClass) {
        this.job.setMapperClass(cls);
        this.job.setMapOutputKeyClass(mapKeyClass);
        this.job.setMapOutputValueClass(mapOutputClass);
        return this;
    }

    public Job build() {
        if (deleteOutIfExists)
            Paths.recursiveDeleteIfExists(outputDir);
        return job;
    }

    public <KEY extends Writable, VALUE extends Writable>  JobBuilder reducerClass(Class<? extends Reducer<? extends Writable, ? extends Writable, KEY, VALUE>> brazilReducerClass, Class<KEY> outputKeyClass, Class<VALUE> outputValueClass) {
        this.job.setReducerClass(brazilReducerClass);
        this.job.setOutputKeyClass(outputKeyClass);
        this.job.setOutputValueClass(outputValueClass);
        return this;
    }

    public JobBuilder combinerClass(Class<? extends Reducer<? extends Writable, ? extends Writable, ? extends Writable, ? extends Writable>> brazilReducerClass) {
        this.job.setCombinerClass(brazilReducerClass);
        return this;
    }
}
