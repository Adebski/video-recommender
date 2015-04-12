package ztis;

import twitter4j.Status;

public class Tweet {
    
    private final String text;
    private final long userId;
    private final String userName;
    private final boolean isRetweet;

    // Needed by Kryo library to instantiate class before populating fields
    private Tweet() {
        this("", -1, "", false);
    }
    
    public Tweet(Status status) {
        this(status.getText(), status.getUser().getId(), status.getUser().getName(), status.isRetweet());
    }

    public Tweet(String text, long userId, String userName, boolean isRetweet) {
        this.text = text;
        this.userId = userId;
        this.userName = userName;
        this.isRetweet = isRetweet;
    }

    public String text() {
        return text;
    }

    public long userId() {
        return userId;
    }

    public String userName() {
        return userName;
    }

    public boolean isRetweet() {
        return isRetweet;
    }

    @Override
    public String toString() {
        return "Tweet{" +
                "text='" + text + '\'' +
                ", userId=" + userId +
                ", userName='" + userName + '\'' +
                ", isRetweet=" + isRetweet +
                '}';
    }
}
