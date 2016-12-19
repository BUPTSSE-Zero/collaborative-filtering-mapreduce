package bupt.sse.hadoop.cfrs.usercf.job;

import bupt.sse.hadoop.cfrs.model.UserSimilarity;
import com.google.gson.Gson;
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
public class Step3ConvertSimilarityMatrixJob {
    private static Gson gson = new Gson();

    public static class JobMapper extends Mapper<Object, Text, IntWritable, Text>{
        @Override
        protected void map(Object ignore, Text inputValue, Context context) throws IOException, InterruptedException {
            String keySplit[] = inputValue.toString().split("/")[0].split(",");
            UserSimilarity us = new UserSimilarity(Integer.parseInt(keySplit[0]), Integer.parseInt(keySplit[1]),
                    Double.parseDouble(inputValue.toString().split("/")[1]));
            Text outputValue = new Text(gson.toJson(us));
            context.write(new IntWritable(us.getUser1Id()), outputValue);
            context.write(new IntWritable(us.getUser2Id()), outputValue);
        }
    }

    public static class JobReducer extends Reducer<IntWritable, Text, IntWritable, IntWritable> {
        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<UserSimilarity> similarityList = new ArrayList<>();
            for(Text t : values){
                similarityList.add(gson.fromJson(t.toString(), UserSimilarity.class));
            }
            Collections.sort(similarityList);
            int userId;
            for(int i = similarityList.size() - 1; i >= 0 && i >= similarityList.size() - 4; i--){
                if(key.get() == similarityList.get(i).getUser1Id())
                    userId = similarityList.get(i).getUser2Id();
                else
                    userId = similarityList.get(i).getUser1Id();
                context.write(new IntWritable(userId), key);
            }
        }
    }
}
