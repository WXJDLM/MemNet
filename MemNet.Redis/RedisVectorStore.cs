using MemNet.Abstractions;
using MemNet.Config;
using MemNet.Models;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
// 保留原有引用（但运行时会忽略RediSearch相关命令）
using NRedisStack;
using NRedisStack.RedisStackCommands;
using NRedisStack.Search;
using NRedisStack.Search.Literals.Enums;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;

namespace MemNet.Redis;

/// <summary>
/// Redis Stack vector store implementation with vector similarity search support
/// </summary>
public class RedisVectorStore : IVectorStore
{

    private readonly ILogger _logger;
    private readonly IConnectionMultiplexer _redis;
    private readonly IDatabase _db;
    private readonly VectorStoreConfig _config;
    private readonly string _indexName;
    private readonly string _keyPrefix;
    // 新增：兼容Windows Redis的索引标记键（仅用于模拟索引存在性，无功能改动）
    private readonly string _indexFlagKey;

    // 核心全局变量：标记Redis是否支持RediSearch（只需判断一次，缓存结果）
    private bool? _isRedisVectorSupported;
    // 线程安全锁：确保只执行一次兼容性判断
    private readonly object _redisSearchCheckLock = new();

    public RedisVectorStore(ILogger<RedisVectorStore> logger,IConnectionMultiplexer redis, IOptions<MemoryConfig> config)
    {
        _logger = logger;
        _redis = redis ?? throw new ArgumentNullException(nameof(redis));
        _config = config.Value.VectorStore;
        _db = _redis.GetDatabase();
        _indexName = $"idx:{_config.CollectionName}";
        _keyPrefix = $"{_config.CollectionName}:";
        // 新增：用普通KEY模拟索引存在性（变量名/逻辑完全不影响原有代码）
        _indexFlagKey = $"flag:{_indexName}";
    }


    /// <summary>
    /// 全局兼容性判断方法：检查Redis是否支持向量查询（开发环境兼容提示，生产环境强制校验）
    /// </summary>
    /// <returns>是否支持向量查询</returns>
    /// <exception cref="NotSupportedException">生产环境Redis不支持向量查询时抛出</exception>
    private async Task<bool> CheckRediSearchSupportAsync()
    {
        // 双重检查锁定：确保线程安全且只执行一次判断
        if (_isRedisVectorSupported.HasValue)
        {
            return _isRedisVectorSupported.Value;
        }

        lock (_redisSearchCheckLock)
        {
            if (_isRedisVectorSupported.HasValue)
            {
                return _isRedisVectorSupported.Value;
            }

            bool isSupported = false;
            try
            {
                // 尝试执行向量查询相关命令判断是否支持Redis向量功能
                SearchCommands ft = _db.FT();
                // 执行测试命令（使用无效索引避免影响实际数据）
                _ = ft._List();

                _isRedisVectorSupported = true;
                isSupported = true;
            }
            catch (RedisServerException ex) when (ex.Message.Contains("unknown command") ||
                                                ex.Message.Contains("FT.SEARCH") ||
                                                ex.Message.Contains("VECTOR_RANGE") ||
                                                ex.Message.Contains("vector"))
            {
                // 捕获到向量查询相关命令未知异常，区分环境处理（接收返回值）
                isSupported = HandleVectorUnsupportedException(ex);
            }
            catch (RedisServerException ex) when (ex.Message.Contains("index not found"))
            {
                // 索引不存在说明命令本身被支持（只是测试索引不存在），标记为支持
                isSupported = true;
            }
            catch (Exception ex)
            {
                // 其他异常区分环境处理（接收返回值）
                isSupported = HandleVectorCheckException(ex);
            }

            _isRedisVectorSupported = isSupported;
            return isSupported;
        }
    }

    /// <summary>
    /// 处理向量查询不支持的异常（开发环境日志提示，生产环境抛异常）
    /// </summary>
    /// <param name="innerException">原始异常</param>
    /// <returns>是否支持向量查询（开发环境返回false，生产环境直接抛异常无返回）</returns>
    /// <exception cref="NotSupportedException">生产环境抛出</exception>
    private bool HandleVectorUnsupportedException(Exception innerException)
    {
        var errorMsg = "当前Redis版本不支持向量查询功能！\n" +
                       "Windows系统用户注意：Windows原生Redis为Demo版本不支持生产环境，建议通过Docker安装Redis Stack（https://redis.io/docs/stack/get-started/install/docker/）。";

        if (IsProductionEnvironment())
        {
            // 生产环境：强制抛出异常，阻断应用启动
            throw new NotSupportedException(
                $"【生产环境禁止启动】{errorMsg}\n请升级Redis至支持RediSearch/Redis Stack的版本后重新部署！",
                innerException);
        }
        else
        {
            Console.WriteLine($"【开发环境兼容提示】{errorMsg}\n该提示仅用于开发兼容，生产环境必须升级Redis！");
           // 开发环境：仅日志警告，返回false表示不支持
           _logger.LogWarning(
                innerException,
                $"【开发环境兼容提示】{errorMsg}\n该提示仅用于开发兼容，生产环境必须升级Redis！");
            return false;
        }
    }

    /// <summary>
    /// 处理向量检测过程中的其他异常（开发环境日志提示，生产环境抛异常）
    /// </summary>
    /// <param name="innerException">原始异常</param>
    /// <returns>是否支持向量查询（开发环境返回false，生产环境直接抛异常无返回）</returns>
    /// <exception cref="NotSupportedException">生产环境抛出</exception>
    private bool HandleVectorCheckException(Exception innerException)
    {
        var errorMsg = "检测Redis向量查询支持时发生异常！\n" +
                       "Windows系统用户请通过Docker安装Redis Stack（https://redis.io/docs/stack/get-started/install/docker/），不要使用Windows原生Redis（仅Demo用途）。";

        if (IsProductionEnvironment())
        {
            // 生产环境：强制抛出异常，阻断应用启动
            throw new NotSupportedException(
                $"【生产环境禁止启动】{errorMsg}\n请确保使用支持RediSearch/Redis Stack的Redis版本后重新部署！",
                innerException);
        }
        else
        {

            Console.WriteLine($"【开发环境兼容提示】{errorMsg}\n该提示仅用于开发兼容，生产环境必须升级Redis！");
            // 开发环境：仅日志警告，返回false表示不支持
            _logger.LogError(
                innerException,
                $"【开发环境兼容提示】{errorMsg}\n该提示仅用于开发兼容，生产环境必须升级Redis！");
            return false;
        }
    }

    /// <summary>
    /// 判断当前是否为生产环境（需根据项目实际配置调整判断逻辑）
    /// </summary>
    /// <returns>是否生产环境</returns>
    private bool IsProductionEnvironment()
    {
        // 方式1：基于ASPNETCORE_ENVIRONMENT环境变量（适用于ASP.NET Core）
        var env = Environment.GetEnvironmentVariable("ASPNETCORE_ENVIRONMENT") ?? string.Empty;
        return env.Equals("Production", StringComparison.OrdinalIgnoreCase);

        // 方式2：基于自定义配置（适用于非Web项目）
        // return _configuration.GetValue<bool>("Environment:IsProduction");

        // 方式3：基于配置文件的环境名称
        // return _configuration["Environment:Name"]?.Equals("Production", StringComparison.OrdinalIgnoreCase) ?? false;
    }

    public async Task EnsureCollectionExistsAsync(int vectorSize, bool allowRecreation, CancellationToken ct = default)
    {
        bool indexExists = false;
        bool isRediSearchSupported = await CheckRediSearchSupportAsync();

        if (isRediSearchSupported)
        {
            // 支持RediSearch：使用原有逻辑
            SearchCommands ft = _db.FT();
            RedisResult[] indexes = ft._List();
            indexExists = indexes.Any(e => _indexName.Equals((string)e!));
        }
        else
        {
            // 不支持RediSearch：使用索引标记键判断
            indexExists = await _db.KeyExistsAsync(_indexFlagKey);
        }

        if (indexExists)
        {
            if (allowRecreation)
            {
                if (isRediSearchSupported)
                {
                    // 支持RediSearch：删除真实索引
                    _ = _db.FT().DropIndex(_indexName);
                }
                else
                {
                    // 不支持RediSearch：删除索引标记
                    _ = await _db.KeyDeleteAsync(_indexFlagKey);
                }
                await CreateIndexAsync(vectorSize);
            }
            return;
        }

        // 索引不存在，创建（兼容模式）
        await CreateIndexAsync(vectorSize);
    }

    // 改动：原CreateIndexAsync参数简化（无功能变化，仅适配兼容逻辑）
    private async Task CreateIndexAsync(int vectorSize)
    {
        bool isRediSearchSupported = await CheckRediSearchSupportAsync();

        if (isRediSearchSupported)
        {
            // 支持RediSearch：使用原有创建逻辑
            SearchCommands ft = _db.FT();
            Schema schema = new Schema()
                .AddTextField("id")
                .AddTextField("data")
                .AddTextField("user_id")
                .AddTextField("hash")
                .AddTextField("metadata")
                .AddNumericField("created_at")
                .AddNumericField("updated_at")
                .AddVectorField("embedding",
                    Schema.VectorField.VectorAlgo.HNSW,
                    new Dictionary<string, object>
                    {
                        ["TYPE"] = "FLOAT32",
                        ["DIM"] = vectorSize,
                        ["DISTANCE_METRIC"] = "COSINE"
                    });

            // 保留原有创建索引逻辑
            bool success = ft.Create(_indexName,
                new FTCreateParams()
                    .On(IndexDataType.HASH)
                    .Prefix(_keyPrefix),
                schema);
            if (!success)
            {
                throw new Exception("Failed to create Redis vector index.");
            }
        }
        else
        {
            // 不支持RediSearch：仅创建索引标记（保留原有逻辑不中断）
            _ = await _db.StringSetAsync(_indexFlagKey, "exists");
        }
    }

    // 以下所有方法完全保留原有逻辑，仅优化SearchAsync和ListAsync的异常处理逻辑
    public async Task InsertAsync(List<MemoryItem> memories, CancellationToken ct = default)
    {
        foreach (MemoryItem memory in memories)
        {
            var key = $"{_keyPrefix}{memory.Id}";
            var hashEntries = new HashEntry[]
            {
                new("id", memory.Id),
                new("data", memory.Data),
                new("user_id", memory.UserId ?? string.Empty),
                new("hash", memory.Hash ?? string.Empty),
                new("metadata", System.Text.Json.JsonSerializer.Serialize(memory.Metadata ?? [])),
                new("created_at", memory.CreatedAt.Ticks),
                new("updated_at", memory.UpdatedAt?.Ticks ?? 0),
                new("embedding", SerializeVector(memory.Embedding))
            };

            await _db.HashSetAsync(key, hashEntries);
        }
    }

    public async Task UpdateAsync(List<MemoryItem> memories, CancellationToken ct = default)
    {
        foreach (MemoryItem memory in memories)
        {
            var key = $"{_keyPrefix}{memory.Id}";

            if (await _db.KeyExistsAsync(key))
            {
                var hashEntries = new HashEntry[]
                {
                    new("data", memory.Data),
                    new("hash", memory.Hash ?? string.Empty),
                    new("metadata", System.Text.Json.JsonSerializer.Serialize(memory.Metadata ?? [])),
                    new("updated_at", memory.UpdatedAt?.Ticks ?? DateTime.UtcNow.Ticks),
                    new("embedding", SerializeVector(memory.Embedding))
                };

                await _db.HashSetAsync(key, hashEntries);
            }
        }
    }

    public async Task<List<MemorySearchResult>> SearchAsync(float[] queryVector, string? userId = null, int limit = 100, CancellationToken ct = default)
    {
        bool isRediSearchSupported = await CheckRediSearchSupportAsync();

        if (isRediSearchSupported)
        {
            // 支持RediSearch：使用原有搜索逻辑
            SearchCommands ft = _db.FT();
            var queryStr = userId != null ? $"@user_id:{EscapeRedisQuery(userId)}" : "*";
            Query query = new Query(queryStr)
                .SetSortBy("__embedding_score")
                .Limit(0, limit)
                .ReturnFields("id", "data", "user_id", "hash", "metadata", "created_at", "updated_at", "embedding", "__embedding_score")
                .Dialect(2);

            var vectorBytes = SerializeVector(queryVector);
            _ = query.AddParam("query_vector", vectorBytes);
            _ = query.AddParam("BLOB", vectorBytes);

            var searchQuery = $"{queryStr}=>[KNN {limit} @embedding $query_vector AS __embedding_score]";
            Query fullQuery = new Query(searchQuery)
                .SetSortBy("__embedding_score")
                .Limit(0, limit)
                .ReturnFields("id", "data", "user_id", "hash", "metadata", "created_at", "updated_at", "__embedding_score")
                .Dialect(2);

            _ = fullQuery.AddParam("query_vector", vectorBytes);

            SearchResult results = ft.Search(_indexName, fullQuery);
            var searchResults = new List<MemorySearchResult>();

            foreach (Document doc in results.Documents)
            {
                MemoryItem memoryItem = ParseMemoryItem(doc);
                RedisValue scoreValue = doc["__embedding_score"];
                var score = !scoreValue.IsNull && float.TryParse(scoreValue.ToString(), out var s)
                    ? 1.0f - s
                    : 0.0f;

                searchResults.Add(new MemorySearchResult
                {
                    Id = memoryItem.Id,
                    Memory = memoryItem,
                    Score = score
                });
            }

            return searchResults;
        }
        else
        {
            // 不支持RediSearch：使用降级搜索逻辑
            return await FallbackSearchAsync(queryVector, userId, limit, ct);
        }
    }

    public async Task<List<MemoryItem>> ListAsync(string? userId = null, int limit = 100, CancellationToken ct = default)
    {
        bool isRediSearchSupported = await CheckRediSearchSupportAsync();

        if (isRediSearchSupported)
        {
            // 支持RediSearch：使用原有列表查询逻辑
            SearchCommands ft = _db.FT();
            var queryStr = userId != null ? $"@user_id:{EscapeRedisQuery(userId)}" : "*";
            Query query = new Query(queryStr)
                .SetSortBy("created_at", false)
                .Limit(0, limit)
                .ReturnFields("id", "data", "user_id", "hash", "metadata", "created_at", "updated_at", "embedding")
                .Dialect(2);

            SearchResult results = ft.Search(_indexName, query);
            return results.Documents.Select(ParseMemoryItem).ToList();
        }
        else
        {
            // 不支持RediSearch：使用降级列表查询逻辑
            return await FallbackListAsync(userId, limit, ct);
        }
    }

    // 新增：降级搜索实现（余弦相似度计算）
    private async Task<List<MemorySearchResult>> FallbackSearchAsync(float[] queryVector, string? userId, int limit, CancellationToken ct)
    {
        // 遍历所有以_keyPrefix开头的Key
        EndPoint[] endpoints = _redis.GetEndPoints();
        IServer server = _redis.GetServer(endpoints.First());
        IEnumerable<RedisKey> keys = server.Keys(_db.Database, $"{_keyPrefix}*", pageSize: limit * 10);

        var candidates = new List<(MemoryItem Item, float Similarity)>();
        foreach (RedisKey key in keys)
        {
            HashEntry[] hash = await _db.HashGetAllAsync(key);
            if (hash.Length == 0)
            {
                continue;
            }

            MemoryItem item = ParseMemoryItem(hash);
            // 过滤用户ID
            if (userId != null && item.UserId != userId)
            {
                continue;
            }

            // 计算余弦相似度
            var similarity = CalculateCosineSimilarity(queryVector, item.Embedding);
            candidates.Add((item, similarity));
        }

        // 按相似度排序取前N
        return candidates.OrderByDescending(x => x.Similarity)
                         .Take(limit)
                         .Select(x => new MemorySearchResult
                         {
                             Id = x.Item.Id,
                             Memory = x.Item,
                             Score = x.Similarity
                         })
                         .ToList();
    }

    // 新增：降级列表查询实现
    private async Task<List<MemoryItem>> FallbackListAsync(string? userId, int limit, CancellationToken ct)
    {
        EndPoint[] endpoints = _redis.GetEndPoints();
        IServer server = _redis.GetServer(endpoints.First());
        IEnumerable<RedisKey> keys = server.Keys(_db.Database, $"{_keyPrefix}*", pageSize: limit);

        var items = new List<MemoryItem>();
        foreach (RedisKey key in keys)
        {
            HashEntry[] hash = await _db.HashGetAllAsync(key);
            if (hash.Length == 0)
            {
                continue;
            }

            MemoryItem item = ParseMemoryItem(hash);
            if (userId == null || item.UserId == userId)
            {
                items.Add(item);
            }
            if (items.Count >= limit)
            {
                break;
            }
        }

        return items.OrderByDescending(x => x.CreatedAt).ToList();
    }

    // 新增：余弦相似度计算
    private float CalculateCosineSimilarity(float[] vec1, float[] vec2)
    {
        if (vec1.Length != vec2.Length || vec1.Length == 0)
        {
            return 0f;
        }

        float dot = 0f, mag1 = 0f, mag2 = 0f;
        for (int i = 0; i < vec1.Length; i++)
        {
            dot += vec1[i] * vec2[i];
            mag1 += vec1[i] * vec1[i];
            mag2 += vec2[i] * vec2[i];
        }

        return mag1 == 0 || mag2 == 0 ? 0f : dot / (float)Math.Sqrt(mag1 * mag2);
    }

    public async Task<MemoryItem?> GetAsync(string memoryId, CancellationToken ct = default)
    {
        var key = $"{_keyPrefix}{memoryId}";

        if (!await _db.KeyExistsAsync(key))
        {
            return null;
        }

        HashEntry[] hash = await _db.HashGetAllAsync(key);

        return hash.Length == 0 ? null : ParseMemoryItem(hash);
    }

    public async Task DeleteAsync(string memoryId, CancellationToken ct = default)
    {
        var key = $"{_keyPrefix}{memoryId}";
        _ = await _db.KeyDeleteAsync(key);
    }

    public async Task DeleteByUserAsync(string userId, CancellationToken ct = default)
    {
        List<MemoryItem> memories = await ListAsync(userId, limit: 10000, ct);

        foreach (MemoryItem memory in memories)
        {
            await DeleteAsync(memory.Id, ct);
        }
    }

    private MemoryItem ParseMemoryItem(Document doc)
    {
        var dict = new Dictionary<string, RedisValue>();
        foreach (var key in new[] { "id", "data", "user_id", "hash", "metadata", "created_at", "updated_at", "embedding" })
        {
            RedisValue value = doc[key];
            if (!value.IsNull)
            {
                dict[key] = value;
            }
        }
        return ParseMemoryItemFromDict(dict);
    }

    private MemoryItem ParseMemoryItem(HashEntry[] hash)
    {
        Dictionary<string, RedisValue> dict = hash.ToDictionary(h => h.Name.ToString(), h => h.Value);
        return ParseMemoryItemFromDict(dict);
    }

    private MemoryItem ParseMemoryItemFromDict(Dictionary<string, RedisValue> dict)
    {
        Dictionary<string, object>? metadata = dict.ContainsKey("metadata") && !string.IsNullOrEmpty(dict["metadata"])
            ? System.Text.Json.JsonSerializer.Deserialize<Dictionary<string, object>>(dict["metadata"])
            : [];

        var embedding = dict.ContainsKey("embedding") && !string.IsNullOrEmpty(dict["embedding"])
            ? DeserializeVector((byte[])dict["embedding"])
            : Array.Empty<float>();

        var createdAtTicks = dict.ContainsKey("created_at") && long.TryParse(dict["created_at"], out var ct)
            ? ct
            : DateTime.UtcNow.Ticks;

        DateTime? updatedAtTicks = dict.ContainsKey("updated_at") && long.TryParse(dict["updated_at"], out var ut) && ut > 0
            ? new DateTime(ut)
            : null;

        return new MemoryItem
        {
            Id = dict.ContainsKey("id") ? dict["id"] : string.Empty,
            Data = dict.ContainsKey("data") ? dict["data"] : string.Empty,
            UserId = dict.ContainsKey("user_id") && !string.IsNullOrEmpty(dict["user_id"]) ? dict["user_id"].ToString() : null,
            Hash = dict.ContainsKey("hash") ? dict["hash"].ToString() : null,
            Metadata = metadata,
            CreatedAt = new DateTime(createdAtTicks),
            UpdatedAt = updatedAtTicks,
            Embedding = embedding
        };
    }

    private byte[] SerializeVector(float[] vector)
    {
        var bytes = new byte[vector.Length * sizeof(float)];
        Buffer.BlockCopy(vector, 0, bytes, 0, bytes.Length);
        return bytes;
    }

    private float[] DeserializeVector(byte[] bytes)
    {
        var floats = new float[bytes.Length / sizeof(float)];
        Buffer.BlockCopy(bytes, 0, floats, 0, bytes.Length);
        return floats;
    }

    private string EscapeRedisQuery(string value)
    {
        return value.Replace("-", "\\-")
                   .Replace(":", "\\:")
                   .Replace("@", "\\@");
    }
}