package twitter4j;

import twitter4j.auth.Authorization;
import twitter4j.conf.Configuration;
import twitter4j.conf.ConfigurationContext;

public class ZTISTwitterFactory {

    private final Configuration conf;

    /**
     * Creates a ZTISTwitterFactory with the root configuration.
     */
    public ZTISTwitterFactory() {
        this(ConfigurationContext.getInstance());
    }

    /**
     * Creates a TwitterFactory with the given configuration.
     *
     * @param conf the configuration to use
     * @since Twitter4J 2.1.1
     */
    public ZTISTwitterFactory(Configuration conf) {
        if (conf == null) {
            throw new NullPointerException("configuration cannot be null");
        }
        this.conf = conf;
    }

    public ZTISTwitter getInstance(Authorization auth) {
        return new ZTISTwitter(conf, auth);
    }
}
