package se.kth.mr;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
/**
 * Created by salman on 5/2/16.
 */
public class Reduce extends Reducer<HdfsStatKey, IntWritable, HdfsStatKey, IntWritable> {
    @Override
    public void reduce(HdfsStatKey key, Iterable<IntWritable> counts, Context context)
            throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable count : counts) {
        sum += count.get();
      }
      context.write(key, new IntWritable(sum));
    }
  }
