package bupt.sse.hadoop.cfrs.usercf.job;

import bupt.sse.hadoop.cfrs.model.UserMovieRating;
import bupt.sse.hadoop.cfrs.model.UserSimilarity;
import com.google.gson.Gson;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author Shengyun Zhou <GGGZ-1101-28@Live.cn>
 */
public class Step2SimilarityMatrixJob {
    public static class JobMapper extends Mapper<Object, Text, Text, DoubleWritable>{
        @Override
        protected void map(Object inputKey, Text inputValue, Context context) throws IOException, InterruptedException {
            int movieId = Integer.valueOf(inputValue.toString().split("/")[0]);
            String value = inputValue.toString().split("/")[1];
            String[] valueSplit = value.split(";");
            UserMovieRating umr1 = new UserMovieRating(Integer.parseInt(valueSplit[0].split(":")[0]),
                    movieId, Float.parseFloat(valueSplit[0].split(":")[1]));
            UserMovieRating umr2 = new UserMovieRating(Integer.parseInt(valueSplit[1].split(":")[0]),
                    movieId, Float.parseFloat(valueSplit[1].split(":")[1]));
            Text outputKey;
            if(umr1.getUserId() < umr2.getUserId())
                outputKey = new Text(umr1.getUserId().toString() + ',' + umr2.getUserId());
            else
                outputKey = new Text(umr2.getUserId().toString() + ',' + umr1.getUserId());
            double result = (double)umr1.getRating() - umr2.getRating();
            result *= result;
            DoubleWritable outputValue = new DoubleWritable(result);
            context.write(outputKey, outputValue);
        }
    }

    public static class JobReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable>{
        @Override
        protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            double sum = 0;
            for(DoubleWritable dw : values){
                sum += dw.get();
                count++;
            }
            double similarity = (double) count / (Math.sqrt(sum) + 1);
            UserSimilarity us = new UserSimilarity(Integer.parseInt(key.toString().split(",")[0]),
                    Integer.parseInt(key.toString().split(",")[1]), similarity);
            context.write(key, new DoubleWritable(similarity));
        }
    }
}
