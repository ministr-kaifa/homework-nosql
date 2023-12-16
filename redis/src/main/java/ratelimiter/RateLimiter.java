package ratelimiter;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.time.Instant;

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
    var windowStart = Instant.ofEpochMilli(Instant.now().toEpochMilli() - timeWindowSeconds * 1_000);
    var inWindowRequestsAmount = 
      redis.lrange(label, 0, maxRequestCount - 1).stream()
        .map(Instant::parse)
        .filter(requestTime -> requestTime.isAfter(windowStart))
        .map(Instant::toString)
        .count();

    if(inWindowRequestsAmount < maxRequestCount) {
      redis.lpush(label, Instant.now().toString());
      redis.ltrim(label, 0, maxRequestCount - 1);
      return true;
    }
    return false;
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
