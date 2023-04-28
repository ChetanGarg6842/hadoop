import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

public class HadoopAnalysis {
    
    public static class AnalysisMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        
        private Text categoryReligionKey = new Text();
        private DoubleWritable dataValue = new DoubleWritable();
        
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split(",");
            
            String category = tokens[0];
            String religion = tokens[1];
            double data = Double.parseDouble(tokens[2]);
            
            categoryReligionKey.set(category + "," + religion);
            dataValue.set(data);
            
            context.write(categoryReligionKey, dataValue);
        }
    }
    
    public static class AnalysisReducer extends Reducer<Text, DoubleWritable, Text, Text> {
        
        private Text result = new Text();
        
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            List<Double> dataList = new ArrayList<Double>();
            double sum = 0.0;
            int count = 0;
            Map<Double, Integer> dataMap = new HashMap<Double, Integer>();
            
            for (DoubleWritable value : values) {
                double data = value.get();
                dataList.add(data);
                sum += data;
                count++;
                if (dataMap.containsKey(data)) {
                    dataMap.put(data, dataMap.get(data) + 1);
                } else {
                    dataMap.put(data, 1);
                }
            }
            
            // Calculate mean
            double mean = sum / count;
            
            // Calculate median
            Collections.sort(dataList);
            double median = 0.0;
            int middle = count / 2;
            if (count % 2 == 0) {
                median = (dataList.get(middle - 1) + dataList.get(middle)) / 2;
            } else {
                median = dataList.get(middle);
            }
            
            // Calculate mode
            double mode = 0.0;
            int maxCount = 0;
            for (Map.Entry<Double, Integer> entry : dataMap.entrySet()) {
                int valueCount = entry.getValue();
                if (valueCount > maxCount) {
                    mode = entry.getKey();
                    maxCount = valueCount;
                }
            }
            
            // Build result string
            String resultString = "Mean: " + mean + ", Median: " + median + ", Mode: " + mode;
            result.set(resultString);
            
            context.write(key, result);
        }
    }
    
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "hadoop_analysis");
         job.setJarByClass(HadoopAnalysis.class);
        job.setMapperClass(AnalysisMapper.class);
        job.setReducerClass(AnalysisReducer.class);
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}