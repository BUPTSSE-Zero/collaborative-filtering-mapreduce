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
import java.text.DecimalFormat;
import java.util.*;

/**
 * @author Shengyun Zhou <GGGZ-1101-28@Live.cn>
 */
public class Step5RecommendMatrixJob {
    private static Gson gson = new Gson();
    private static final String MAPPER_OUTPUT_USER_RATING_PREFIX = "UserRating";
    private static final String MAPPER_OUTPUT_RECOMMNEDER_RATING_LIST_PREFIX = "RecommenderRatingList";

    public static class JobMapper extends Mapper<Object, Text, IntWritable, Text>{
        private static final int INPUT_TYPE_STEP4_OUTPUT = 1;
        private static final int INPUT_TYPE_ORIGIN_DATA = 2;
        private int inputDataType = -1;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            inputDataType = -1;
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            if(fileSplit.getPath().getParent().getName().toLowerCase().equals("step4"))
                inputDataType = INPUT_TYPE_STEP4_OUTPUT;
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
                case INPUT_TYPE_STEP4_OUTPUT:
                    int recommender = Integer.parseInt(inputValue.toString().split("/")[0].split(",")[1]);
                    int mainUserId = Integer.parseInt(inputValue.toString().split("/")[0].split(",")[0]);
                    String[] valueSplit = inputValue.toString().split("/")[1].split(";");
                    List<UserMovieRating> ratingList = new ArrayList<>();
                    for(String str : valueSplit){
                        ratingList.add(new UserMovieRating(recommender, Integer.parseInt(str.split(":")[0]), Float.parseFloat(str.split(":")[1])));
                    }
                    context.write(new IntWritable(mainUserId), new Text(MAPPER_OUTPUT_RECOMMNEDER_RATING_LIST_PREFIX + gson.toJson(ratingList)));
                    break;
            }
        }
    }

    public static class JobReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
        private static DecimalFormat df = new DecimalFormat("#.##");

        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Map<Integer, List<UserMovieRating>> movieRating = new HashMap<>();
            Map<Integer, Boolean> watchedMovie = new HashMap<>();
            List<RecommendItem> resultList = new ArrayList<>();
            int count = 0;

            for(Text t : values){
                if(t.toString().startsWith(MAPPER_OUTPUT_USER_RATING_PREFIX)) {
                    UserMovieRating umr = gson.fromJson(t.toString().substring(MAPPER_OUTPUT_USER_RATING_PREFIX.length()), UserMovieRating.class);
                    watchedMovie.put(umr.getMovieId(), true);
                }
                else if(t.toString().startsWith(MAPPER_OUTPUT_RECOMMNEDER_RATING_LIST_PREFIX)){
                    List<UserMovieRating> ratingList = gson.fromJson(t.toString().substring(MAPPER_OUTPUT_RECOMMNEDER_RATING_LIST_PREFIX.length()),
                            new TypeToken<List<UserMovieRating>>(){}.getType());
                    count++;
                    for(UserMovieRating umr : ratingList){
                        List<UserMovieRating> valueRatingList = movieRating.get(umr.getMovieId());
                        if(valueRatingList == null){
                            valueRatingList = new ArrayList<>();
                            movieRating.put(umr.getMovieId(), valueRatingList);
                        }
                        valueRatingList.add(umr);
                    }
                }
            }
            for(Map.Entry<Integer, List<UserMovieRating>> e : movieRating.entrySet()){
                if(watchedMovie.containsKey(e.getKey()))
                    continue;
                float ratingSum = 0;
                for(UserMovieRating umr : e.getValue()){
                    ratingSum += umr.getRating();
                }
                resultList.add(new RecommendItem(e.getKey(), ratingSum / count));
            }
            Collections.sort(resultList);
            if(resultList.isEmpty()){
                context.write(key, new Text("None"));
                return;
            }
            String outputValue = "";
            for(int i = resultList.size() - 1; i >= 0 && i >= resultList.size() - 4; i--){
                if(i < resultList.size() - 1)
                    outputValue += ";";
                outputValue += resultList.get(i).getItemId().toString() + ':' + df.format(resultList.get(i).getRecommendRating());
            }
            context.write(key, new Text(outputValue));
        }
    }
}
