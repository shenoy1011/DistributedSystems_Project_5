import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class q1 {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      /*
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        context.write(word, one);
      }
      */

      String line = value.toString();

      if (line.length() != 0 && line.charAt(0) == 'T') {
        StringTokenizer itr = new StringTokenizer(line);
        itr.nextToken();
        itr.nextToken();
        String time = itr.nextToken();
        if (time.substring(0,2).equals("00")) {
          word.set("0:00 - 0:59");
          context.write(word, one);
        } else if (time.substring(0,2).equals("01")) {
          word.set("1:00 - 1:59");
          context.write(word, one);
        } else if (time.substring(0,2).equals("02")) {
          word.set("2:00 - 2:59");
          context.write(word, one);
        } else if (time.substring(0,2).equals("03")) {
          word.set("3:00 - 3:59");
          context.write(word, one);
        } else if (time.substring(0,2).equals("04")) {
          word.set("4:00 - 4:59");
          context.write(word, one);
        } else if (time.substring(0,2).equals("05")) {
          word.set("5:00 - 5:59");
          context.write(word, one);
        } else if (time.substring(0,2).equals("06")) {
          word.set("6:00 - 6:59");
          context.write(word, one);
        } else if (time.substring(0,2).equals("07")) {
          word.set("7:00 - 7:59");
          context.write(word, one);
        } else if (time.substring(0,2).equals("08")) {
          word.set("8:00 - 8:59");
          context.write(word, one);
        } else if (time.substring(0,2).equals("09")) {
          word.set("9:00 - 9:59");
          context.write(word, one);
        } else if (time.substring(0,2).equals("10")) {
          word.set("10:00 - 10:59");
          context.write(word, one);
        } else if (time.substring(0,2).equals("11")) {
          word.set("11:00 - 11:59");
          context.write(word, one);
        } else if (time.substring(0,2).equals("12")) {
          word.set("12:00 - 12:59");
          context.write(word, one);
        } else if (time.substring(0,2).equals("13")) {
          word.set("13:00 - 13:59");
          context.write(word, one);
        } else if (time.substring(0,2).equals("14")) {
          word.set("14:00 - 14:59");
          context.write(word, one);
        } else if (time.substring(0,2).equals("15")) {
          word.set("15:00 - 15:59");
          context.write(word, one);
        } else if (time.substring(0,2).equals("16")) {
          word.set("16:00 - 16:59");
          context.write(word, one);
        } else if (time.substring(0,2).equals("17")) {
          word.set("17:00 - 17:59");
          context.write(word, one);
        } else if (time.substring(0,2).equals("18")) {
          word.set("18:00 - 18:59");
          context.write(word, one);
        } else if (time.substring(0,2).equals("19")) {
          word.set("19:00 - 19:59");
          context.write(word, one);
        } else if (time.substring(0,2).equals("20")) {
          word.set("20:00 - 20:59");
          context.write(word, one);
        } else if (time.substring(0,2).equals("21")) {
          word.set("21:00 - 21:59");
          context.write(word, one);
        } else if (time.substring(0,2).equals("22")) {
          word.set("22:00 - 22:59");
          context.write(word, one);
        } else if (time.substring(0,2).equals("23")) {
          word.set("23:00 - 23:59");
          context.write(word, one);
        } // else if
      } // if
    } // map
  } // TokenizerMapper

  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    //conf.set("textinputformat.record.delimiter", "\n\nT");
    Job job = Job.getInstance(conf, "q1");
    job.setJarByClass(q1.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}