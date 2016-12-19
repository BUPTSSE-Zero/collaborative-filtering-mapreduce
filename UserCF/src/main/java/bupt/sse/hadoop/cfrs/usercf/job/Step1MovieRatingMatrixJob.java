package bupt.sse.hadoop.cfrs.usercf.job;

import bupt.sse.hadoop.cfrs.model.UserMovieRating;
import com.google.gson.Gson;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Shengyun Zhou <GGGZ-1101-28@Live.cn>
 */
public class Step1MovieRatingMatrixJob {
    private static Gson gson = new Gson();

    public static class JobMapper extends Mapper<Object, Text, IntWritable, Text>{
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] splitStr = value.toString().split(",");
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
            List<UserMovieRating> ratingList = new ArrayList<>();
            for(Text t: values){
                ratingList.add(gson.fromJson(t.toString(), UserMovieRating.class));
            }
            for(int i = 0; i < ratingList.size(); i++){
                for(int j = i + 1; j < ratingList.size(); j++){
                    if(ratingList.get(i).getUserId().equals(ratingList.get(j).getUserId()))
                        continue;
                    Text outputValue = new Text(ratingList.get(i).getUserId().toString() + ':' + ratingList.get(i).getRating() + ';' +
                            ratingList.get(j).getUserId().toString() + ':' + ratingList.get(j).getRating());
                    context.write(key, outputValue);
                }
            }
        }
    }
}
