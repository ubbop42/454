import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.commons.lang.StringUtils;
import java.util.*;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Task1 {
    

  // add code here
    public static class Task1Mapper extends Mapper<Object, Text, Text, Text>{
        
        private Text movieTitle = new Text();
        private Text userWithMaxRatings = new Text();
            
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split(",", -1);
            movieTitle.set(tokens[0]);
            Integer[] rankings = new Integer[tokens.length - 1];
            int max = 0;
            
            for(int i = 0;i < rankings.length; i++)        
            {
                try 
                {
                    rankings[i] = Integer.parseInt(tokens[i+1]);
                    if(rankings[i] > max) max = rankings[i];
                }
                catch (NumberFormatException nfe)   
                {
                    rankings[i] = 0;
                }
            }
            
            ArrayList<Integer> users = new ArrayList<Integer>();
            
            for (int i = 0; i < rankings.length; i++) 
            {
                if (rankings[i] == max) {
                    users.add(i+1);
                }
            }

            userWithMaxRatings.set(StringUtils.join(users, ','));
            context.write(movieTitle, userWithMaxRatings);
        }
    }
    
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.set("mapreduce.output.textoutputformat.separator", ",");
    
    Job job = Job.getInstance(conf, "Task1");
    job.setJarByClass(Task1.class);

    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

    job.setMapperClass(Task1Mapper.class);
    job.setNumReduceTasks(0);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    TextInputFormat.addInputPath(job, new Path(otherArgs[0]));
    TextOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
