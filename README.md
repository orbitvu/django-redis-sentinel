# django-redis-sentinel
Plugin for django-redis that supports Redis Sentinel

# Installation

```
pip install django-redis-sentinel
```

# Usage

In your settings, do something like this:

```
    CACHES = {
        "default": {
            "BACKEND": "django_redis.cache.RedisCache",
            "LOCATION": "redis_master/sentinel-host1.yourdomain.com:2639,sentinel-host2.yourdomain.com:2639"
            "OPTIONS": {
                "CLIENT_CLASS": "django_redis_sentinel.SentinelClient",
            }
        }
    }
```
