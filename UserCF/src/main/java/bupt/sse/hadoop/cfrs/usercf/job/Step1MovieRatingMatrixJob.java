package bupt.sse.hadoop.cfrs.usercf.job;

import bupt.sse.hadoop.cfrs.model.UserMovieRating;
import com.google.gson.Gson;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author Shengyun Zhou <GGGZ-1101-28@Live.cn>
 */
public class Step1MovieRatingMatrixJob {
    private static Gson gson = new Gson();

    public static class JobMapper extends Mapper<Object, Text, IntWritable, Text>{
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] splitStr = value.toString().split("::");
            UserMovieRating umr = new UserMovieRating(Integer.parseInt(splitStr[0]),
                    Integer.parseInt(splitStr[1]), Float.parseFloat(splitStr[2]));
            IntWritable outputKey = new IntWritable(umr.getMovieId());
            Text outputValue = new Text(gson.toJson(umr));
            context.write(outputKey, outputValue);
        }
    }

    public static class JobReducer extends Reducer<IntWritable, Text, IntWritable, Text>{
        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for(Text t: values){
                UserMovieRating umr = gson.fromJson(t.toString(), UserMovieRating.class);
                context.write(key, new Text(String.valueOf(umr.getUserId()) + ':' + String.valueOf(umr.getRating())));
            }
        }
    }
}
