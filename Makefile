# 定义编译器
CC=gcc
# 定义编译选项，包括头文件搜索路径和-fPIC为了生成位置无关代码
CFLAGS=-I./deps/murmur2 -I./RedisBloom/deps/murmur2 -I./RedisBloom/deps -I./RedisBloom/src -I./RedisBloom/deps/RedisModulesSDK/rmutil -fPIC 
# 链接选项，-shared生成动态链接库
LDFLAGS=-shared -lm

# 输出的动态链接库名
TARGET=lib/libbloom_filter.so

# 源文件
SRC=RedisBloom/deps/bloom/bloom.c RedisBloom/deps/murmur2/MurmurHash2.c RedisBloom/src/cf.c  RedisBloom/src/sb.c RedisBloom/deps/RedisModulesSDK/rmutil/sds.c RedisBloom/deps/RedisModulesSDK/rmutil/sds.h

# 默认目标
all: before_build $(TARGET)

# 检查并创建lib目录
before_build:
	mkdir -p lib

# 目标规则
$(TARGET): $(SRC)
	$(CC) $(LDFLAGS) $(CFLAGS) -o $@ $(SRC)

# 清理编译生成的文件
clean:
	rm -f $(TARGET)
