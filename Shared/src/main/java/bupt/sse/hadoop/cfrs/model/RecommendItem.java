package bupt.sse.hadoop.cfrs.model;

/**
 * @author Shengyun Zhou <GGGZ-1101-28@Live.cn>
 */
public class RecommendItem implements Comparable<RecommendItem> {
    private Integer itemId;
    private Float recommendRating;

    public RecommendItem(Integer itemId, Float recommendRating) {
        this.itemId = itemId;
        this.recommendRating = recommendRating;
    }

    public Integer getItemId() {
        return itemId;
    }

    public void setItemId(Integer itemId) {
        this.itemId = itemId;
    }

    public Float getRecommendRating() {
        return recommendRating;
    }

    public void setRecommendRating(Float recommendRating) {
        this.recommendRating = recommendRating;
    }

    @Override
    public int compareTo(RecommendItem o) {
        if(recommendRating < o.recommendRating)
            return -1;
        else if(recommendRating > o.recommendRating)
            return 1;
        return 0;
    }
}
