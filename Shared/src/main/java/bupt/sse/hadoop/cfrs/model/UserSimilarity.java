package bupt.sse.hadoop.cfrs.model;

/**
 * @author Shengyun Zhou <GGGZ-1101-28@Live.cn>
 */
public class UserSimilarity implements Comparable<UserSimilarity> {
    private Integer user1Id;
    private Integer user2Id;
    private double similarity;

    public UserSimilarity(Integer user1Id, Integer user2Id, double similarity) {
        this.user1Id = user1Id;
        this.user2Id = user2Id;
        this.similarity = similarity;
    }


    public Integer getUser1Id() {
        return user1Id;
    }

    public void setUser1Id(Integer user1Id) {
        this.user1Id = user1Id;
    }

    public Integer getUser2Id() {
        return user2Id;
    }

    public void setUser2Id(Integer user2Id) {
        this.user2Id = user2Id;
    }

    public double getSimilarity() {
        return similarity;
    }

    public void setSimilarity(double similarity) {
        this.similarity = similarity;
    }

    @Override
    public int compareTo(UserSimilarity o) {
        if(similarity < o.similarity)
            return -1;
        else if(similarity > o.similarity)
            return 1;
        return 0;
    }
}
