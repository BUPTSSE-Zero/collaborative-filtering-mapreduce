package bupt.sse.hadoop.cfrs.model;

/**
 * @author Shengyun Zhou <GGGZ-1101-28@Live.cn>
 */
public class UserMovieRating {
    private Integer userId;
    private Integer movieId;
    private Float rating;

    public UserMovieRating(Integer userId, Integer movieId, Float rating) {
        this.userId = userId;
        this.movieId = movieId;
        this.rating = rating;
    }

    public Integer getUserId() {
        return userId;
    }

    public void setUserId(Integer userId) {
        this.userId = userId;
    }

    public Integer getMovieId() {
        return movieId;
    }

    public void setMovieId(Integer movieId) {
        this.movieId = movieId;
    }

    public Float getRating() {
        return rating;
    }

    public void setRating(Float rating) {
        this.rating = rating;
    }
}
