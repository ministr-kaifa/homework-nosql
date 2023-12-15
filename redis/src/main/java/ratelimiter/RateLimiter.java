package ratelimiter;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.time.Instant;
import java.util.Objects;
import java.util.stream.Stream;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public class RateLimiter {

  private final Jedis redis;
  private final String label;
  private final long maxRequestCount;
  private final long timeWindowSeconds;

  public RateLimiter(Jedis redis, String label, long maxRequestCount, long timeWindowSeconds) {
    this.redis = redis;
    this.label = label;
    this.maxRequestCount = maxRequestCount;
    this.timeWindowSeconds = timeWindowSeconds;
  }

  public boolean pass() {
    var lastRequestsTime = 
      Stream.generate(() -> redis.lpop(label))
        .takeWhile(val -> Objects.nonNull(val))
        .limit(maxRequestCount)
        .map(listValue -> Instant.parse(listValue))
        .filter(instant -> instant.isAfter(Instant.ofEpochMilli(Instant.now().toEpochMilli() - timeWindowSeconds * 1_000)))
        .toList();
    //хотел сделать через .peek() но не придумал как
    lastRequestsTime.forEach(requestTime -> redis.lpush(label, requestTime.toString()));
    if(lastRequestsTime.size() < maxRequestCount) {
      redis.lpush(label, Instant.now().toString());
      return true;
    } else {
      return false;
    }
  }

  public static void main(String[] args) {
    JedisPool pool = new JedisPool("localhost", 6379);

    try (Jedis redis = pool.getResource()) {
      RateLimiter rateLimiter = new RateLimiter(redis, "pr_rate", 1, 1);

      BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
      long prev = Instant.now().toEpochMilli();
      long now;

      while (true) {
        try {
          String s = br.readLine();
          if (s == null || s.equals("q")) {
            return;
          }
          boolean passed = rateLimiter.pass();

          now = Instant.now().toEpochMilli();
          if (passed) {
            System.out.printf("%d ms: %s", now - prev, "passed");
            prev = now;
          } else {
            System.out.printf("%d ms: %s", now - prev, "limited");
          }
        } catch (IOException e) {
          e.printStackTrace();
        }
      }

    }
  }
}
