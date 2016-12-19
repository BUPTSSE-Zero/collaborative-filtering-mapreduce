package bupt.sse.hadoop.cfrs.usercf.job;

import bupt.sse.hadoop.cfrs.model.RecommendItem;
import bupt.sse.hadoop.cfrs.model.UserMovieRating;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.*;

/**
 * @author Shengyun Zhou <GGGZ-1101-28@Live.cn>
 */
public class Step4RecommenderRatingMatrixJob {
    private static Gson gson = new Gson();
    private static final String MAPPER_OUTPUT_USER_RATING_PREFIX = "UserRating";
    private static final String MAPPER_OUTPUT_USER_ID_PREFIX = "UserId";

    public static class JobMapper extends Mapper<Object, Text, IntWritable, Text>{
        private static final int INPUT_TYPE_STEP3_OUTPUT = 1;
        private static final int INPUT_TYPE_ORIGIN_DATA = 2;
        private int inputDataType = -1;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            inputDataType = -1;
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            if(fileSplit.getPath().getParent().getName().toLowerCase().equals("step3"))
                inputDataType = INPUT_TYPE_STEP3_OUTPUT;
            else if(fileSplit.getPath().getParent().getName().toLowerCase().equals("input"))
                inputDataType = INPUT_TYPE_ORIGIN_DATA;
        }

        @Override
        protected void map(Object ignore, Text inputValue, Context context) throws IOException, InterruptedException {
            switch (inputDataType){
                case INPUT_TYPE_ORIGIN_DATA:
                    String[] splitStr = inputValue.toString().split(",");
                    UserMovieRating umr = new UserMovieRating(Integer.parseInt(splitStr[0]),
                            Integer.parseInt(splitStr[1]), Float.parseFloat(splitStr[2]));
                    IntWritable outputKey = new IntWritable(umr.getUserId());
                    Text outputValue = new Text(MAPPER_OUTPUT_USER_RATING_PREFIX + gson.toJson(umr));
                    context.write(outputKey, outputValue);
                    break;
                case INPUT_TYPE_STEP3_OUTPUT:
                    int recommender = Integer.parseInt(inputValue.toString().split("/")[0]);
                    int mainUserId = Integer.parseInt(inputValue.toString().split("/")[1]);
                    context.write(new IntWritable(recommender), new Text(MAPPER_OUTPUT_USER_ID_PREFIX + mainUserId));
                    break;
            }
        }
    }

    public static class JobReducer extends Reducer<IntWritable, Text, Text, Text>{
        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<UserMovieRating> ratingList =  new ArrayList<>();
            List<Integer> recommandList = new ArrayList<>();

            for(Text t : values){
                if(t.toString().startsWith(MAPPER_OUTPUT_USER_RATING_PREFIX)) {
                    ratingList.add(gson.fromJson(t.toString().substring(MAPPER_OUTPUT_USER_RATING_PREFIX.length()), UserMovieRating.class));
                }
                else if(t.toString().startsWith(MAPPER_OUTPUT_USER_ID_PREFIX)){
                    recommandList.add(Integer.parseInt(t.toString().substring(MAPPER_OUTPUT_USER_ID_PREFIX.length())));
                }
            }

            String outputRatingValue = "";
            for(int i = 0; i < ratingList.size(); i++){
                if(i > 0)
                    outputRatingValue += ";";
                outputRatingValue += ratingList.get(i).getMovieId().toString() + ':' + ratingList.get(i).getRating().toString();
            }
            Text outputValue = new Text(outputRatingValue);

            for(int user : recommandList){
                context.write(new Text(String.valueOf(user) + ',' + key.get()), outputValue);
            }
        }
    }
}
