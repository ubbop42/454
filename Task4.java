import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Task4 {
  public static class Task4Mapper extends Mapper<Object, Text, Text, ArrayPrimitiveWritable> {
    private Text movieTitle = new Text();
    private final static IntWritable rating = new IntWritable();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

      String[] tokens = value.toString().split(",", -1);
      int[] ratings = new int[tokens.length - 1];

      movieTitle.set(tokens[0]);

        for(int i = 0;i < ratings.length; i++)        
        {
            try 
            {
                ratings[i] = Integer.parseInt(tokens[i+1]);
            }
            catch (NumberFormatException nfe)   
            {
                ratings[i] = 0;
            }
        }

      context.write(movieTitle, new ArrayPrimitiveWritable(ratings));
    }

  }


  public static class Task4Reducer extends Reducer<Text, ArrayPrimitiveWritable, Text, Text> {
    private static HashMap<String, int[]> inMemory = new HashMap<>();
    private static Text moviePair = new Text();
    private static Text similarityCount = new Text();


    public void reduce(Text key, Iterable<ArrayPrimitiveWritable> values, Context context) throws IOException, InterruptedException {
      String movieTitle = key.toString();

      int[] ratings = (int[]) values.iterator().next().get();
        
      for (Map.Entry<String, int[]> entry : inMemory.entrySet()) {
        String Pair;
        if (movieTitle.compareTo(entry.getKey()) < 0) {
            Pair = movieTitle + "," + entry.getKey();
        } else {
            Pair = entry.getKey() + "," + movieTitle;
        }
        int sum = 0;
        int[] ratingsBeta = entry.getValue();
          for (int i = 0; i < ratings.length; i++) {
            if(ratings[i] == ratingsBeta[i] && ratings[i] != 0){
                sum++;
            }
          }

        moviePair.set(Pair);
        similarityCount.set(Integer.toString(sum));
        context.write(moviePair, similarityCount);
      }
      inMemory.put(movieTitle, ratings);
    }

  }


  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.set("mapred.textoutputformat.separator", ",");
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    Job job = new Job(conf, "Task 4");
      
    job.setJarByClass(Task4.class);
    job.setMapperClass(Task4Mapper.class);
    job.setReducerClass(Task4Reducer.class);
      
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(ArrayPrimitiveWritable.class);
    TextInputFormat.addInputPath(job, new Path(otherArgs[0]));
    TextOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}