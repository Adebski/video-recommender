package twitter4j;

import twitter4j.auth.Authorization;
import twitter4j.conf.Configuration;
import twitter4j.internal.http.HttpParameter;
import twitter4j.internal.http.HttpResponse;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Because of dependency on spark-streaming-twitter we can't add to our classpath 4.0.X version of twitter4j
 * that has a method for getting a list of followers with count parameter.
 * 
 * Because of that this ugly class was created that adds a method for getting followers list that uses maximum count.
 */
public class ZTISTwitter extends TwitterImpl {
    private final String IMPLICIT_PARAMS_STR;
    private final HttpParameter[] IMPLICIT_PARAMS;
    private final HttpParameter INCLUDE_MY_RETWEET;

    private static final Map<Configuration, HttpParameter[]> implicitParamsMap = new HashMap<Configuration, HttpParameter[]>();
    private static final Map<Configuration, String> implicitParamsStrMap = new HashMap<Configuration, String>();
    private static final int MAX_COUNT = 200;
    
    /*package*/
    public ZTISTwitter(Configuration conf, Authorization auth) {
        super(conf, auth);
        INCLUDE_MY_RETWEET = new HttpParameter("include_my_retweet", conf.isIncludeMyRetweetEnabled());
        HttpParameter[] implicitParams = implicitParamsMap.get(conf);
        String implicitParamsStr = implicitParamsStrMap.get(conf);
        if (implicitParams == null) {
            String includeEntities = conf.isIncludeEntitiesEnabled() ? "1" : "0";
            String includeRTs = conf.isIncludeRTsEnabled() ? "1" : "0";
            boolean contributorsEnabled = conf.getContributingTo() != -1L;
            implicitParamsStr = "include_entities=" + includeEntities + "&include_rts=" + includeRTs
                    + (contributorsEnabled ? "&contributingto=" + conf.getContributingTo() : "");
            implicitParamsStrMap.put(conf, implicitParamsStr);

            List<HttpParameter> params = new ArrayList<HttpParameter>();
            params.add(new HttpParameter("include_entities", includeEntities));
            params.add(new HttpParameter("include_rts", includeRTs));
            if (contributorsEnabled) {
                params.add(new HttpParameter("contributingto", conf.getContributingTo()));
            }
            implicitParams = params.toArray(new HttpParameter[params.size()]);
            implicitParamsMap.put(conf, implicitParams);
        }
        IMPLICIT_PARAMS = implicitParams;
        IMPLICIT_PARAMS_STR = implicitParamsStr;
    }
    
    public PagableResponseList<User> getFollowersListMaxCount(long userId, long cursor) throws TwitterException {
        return factory.createPagableUserList(get(conf.getRestBaseURL() + "followers/list.json?user_id=" + userId
                + "&cursor=" +  cursor + "&count=" + MAX_COUNT));
    }

    private HttpResponse get(String url) throws TwitterException {
        ensureAuthorizationEnabled();
        if (url.contains("?")) {
            url = url + "&" + IMPLICIT_PARAMS_STR;
        } else {
            url = url + "?" + IMPLICIT_PARAMS_STR;
        }
        if (!conf.isMBeanEnabled()) {
            return http.get(url, auth);
        } else {
            // intercept HTTP call for monitoring purposes
            HttpResponse response = null;
            long start = System.currentTimeMillis();
            try {
                response = http.get(url, auth);
            } finally {
                long elapsedTime = System.currentTimeMillis() - start;
                TwitterAPIMonitor.getInstance().methodCalled(url, elapsedTime, isOk(response));
            }
            return response;
        }
    }

    private boolean isOk(HttpResponse response) {
        return response != null && response.getStatusCode() < 300;
    }
}
