package ztis.twitter;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;

public class Tweet {

    private final long userId;

    private final String userName;

    private final boolean isRetweet;

    private final String[] videoLinks;

    private final String timestamp;

    // Needed by Kryo library to instantiate class before populating fields
    private Tweet() {
        this(-1, "", new String[0], false);
    }

    public Tweet(long userId, String userName, String[] videoLinks, boolean isRetweet) {
        this.userId = userId;
        this.userName = userName;
        this.videoLinks = videoLinks;
        this.isRetweet = isRetweet;
        this.timestamp = LocalDateTime.now(ZoneOffset.UTC).toString();
    }

    public long userId() {
        return userId;
    }

    public String userName() {
        return userName;
    }

    public String[] videoLinks() {
        return videoLinks;
    }

    public String timestamp() {
        return timestamp;
    }

    @Override public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        Tweet tweet = (Tweet)o;

        if (userId != tweet.userId)
            return false;
        if (isRetweet != tweet.isRetweet)
            return false;
        if (!userName.equals(tweet.userName))
            return false;
        // Probably incorrect - comparing Object[] arrays with Arrays.equals
        if (!Arrays.equals(videoLinks, tweet.videoLinks))
            return false;
        return timestamp.equals(tweet.timestamp);

    }

    @Override public int hashCode() {
        int result = (int)(userId ^ (userId >>> 32));
        result = 31 * result + userName.hashCode();
        result = 31 * result + (isRetweet ? 1 : 0);
        result = 31 * result + Arrays.hashCode(videoLinks);
        result = 31 * result + timestamp.hashCode();
        return result;
    }

    @Override public String toString() {
        return "Tweet{" +
                "userId=" + userId +
                ", userName='" + userName + '\'' +
                ", isRetweet=" + isRetweet +
                ", videoLinks=" + Arrays.toString(videoLinks) +
                ", timestamp='" + timestamp + '\'' +
                '}';
    }
}
