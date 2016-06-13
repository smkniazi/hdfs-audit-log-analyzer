package se.kth;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
/**
 * Created by salman on 5/2/16.
 */
public class Reduce extends Reducer<HdfsStatKey, LongWritable, HdfsStatKey, LongWritable> {
    @Override
    public void reduce(HdfsStatKey key, Iterable<LongWritable> counts, Context context)
            throws IOException, InterruptedException {
      long sum = 0;
      for (LongWritable count : counts) {
        sum += count.get();
      }
      context.write(key, new LongWritable(sum));
    }
  }
