# 创建manggo用户demo
docker exec mongodb mongosh --quiet --eval '
use demo;
db.createUser({
  user: "demo",
  pwd: "123456",
  roles: [{ role: "readWrite", db: "demo" }]
});
print("---");
use demo;
db.getUsers();
' 2>&1


# 创建mongodb用户demo
docker exec mongodb mongosh "mongodb://127.0.0.1:27017/admin" --quiet --eval '
  db.createUser({
    user: "demo",
    pwd: "123456",
    roles: [{ role: "readWrite", db: "demo" }]
  })
'

# 安装kafka-map
docker run -d --name kafka-map -p 8888:8080 \
    --restart always \
    --network 1panel-network \
    -e DEFAULT_USERNAME=demo \
    -e DEFAULT_PASSWORD=123456 \
    -v /opt/data/kafka-map:/usr/local/kafka-map/data \
    -d dushixiang/kafka-map:latest

# 关闭redis保护模式(重启失效)
docker exec redis redis-cli CONFIG SET protected-mode no

# 关闭redis保护模式(永久生效)(修改/etc/redis/redis.conf)
docker exec redis redis-cli CONFIG SET protected-mode no
docker exec redis redis-cli CONFIG REWRITE
docker exec redis redis-cli CONFIG GET protected-mode

# 关闭redis保护模式(永久生效)
docker exec redis sed -i 's/^protected-mode yes/protected-mode no/' /etc/redis/redis.conf
docker exec redis grep "protected-mode" /etc/redis/redis.conf
docker restart redis
