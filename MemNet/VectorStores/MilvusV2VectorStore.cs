using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Net.Http.Json;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading;
using System.Threading.Tasks;
using MemNet.Abstractions;
using MemNet.Config;
using MemNet.Internals;
using MemNet.Models;
using Microsoft.Extensions.Options;

namespace MemNet.VectorStores;

/// <summary>
/// Milvus vector store implementation using REST API V2
/// </summary>
public class MilvusV2VectorStore : IVectorStore
{
    private readonly HttpClient _httpClient;
    private readonly VectorStoreConfig _config;
    private readonly string _collectionName;

    public MilvusV2VectorStore(HttpClient httpClient, IOptions<MemoryConfig> config)
    {
        _httpClient = httpClient;
        _config = config.Value.VectorStore;
        _collectionName = _config.CollectionName;

        // Configure HttpClient
        if (_httpClient.BaseAddress == null)
        {
            _httpClient.BaseAddress = new Uri(_config.Endpoint);
        }

        if (!string.IsNullOrEmpty(_config.ApiKey))
        {
            _httpClient.DefaultRequestHeaders.Add("Authorization", $"Bearer {_config.ApiKey}");
        }
    }

    public async Task EnsureCollectionExistsAsync(int vectorSize, bool allowRecreation, CancellationToken ct = default)
    {
        // Check if collection exists
        var checkResponse = await _httpClient.PostAsJsonAsync("/v2/vectordb/collections/describe", 
            new { collectionName = _collectionName }, ct);
        await checkResponse.EnsureSuccessWithContentAsync();
        var content = await checkResponse.Content.ReadAsStringAsync();
        MilvusV2Response<MilvusV2CollectionInfo>? responseObj = JsonSerializer.Deserialize<MilvusV2Response<MilvusV2CollectionInfo>>(content);
        if (responseObj != null && responseObj.Code == 0)
        {
            var existingDimension = responseObj.Data?.Fields
                .SingleOrDefault(f => f.Name == "embedding")?.Params.SingleOrDefault(p=>p.Key=="dim")?.Value;
            
            if (Convert.ToInt32(existingDimension) != vectorSize)
            {
                if (allowRecreation)
                {
                    // Delete and recreate with correct dimension
                    await _httpClient.PostAsJsonAsync("/v2/vectordb/collections/drop", 
                        new { collectionName = _collectionName }, ct);
                    await CreateCollectionAsync(vectorSize, ct);
                }
                else
                {
                    throw new InvalidOperationException(
                        $"Collection '{_collectionName}' exists with dimension {existingDimension}, but {vectorSize} was requested. " +
                        "Set allowRecreation=true to automatically recreate the collection.");
                }
            }
        }
        else if (responseObj != null && responseObj.Code == 100) //collection not found 
        {
            // Collection doesn't exist, create it
            await CreateCollectionAsync(vectorSize, ct);
        }
        else
        {
            throw new InvalidOperationException(
                $"Milvus returned error code {responseObj?.Code}: {responseObj?.Message}. Response: {content}");
        }
    }

    private async Task CreateCollectionAsync(int vectorSize, CancellationToken ct)
    {
        var createRequest = new
        {
            collectionName = _collectionName,
            schema = new
            {
                fields = new object[]
                {
                    new { fieldName = "id", dataType = "VarChar", isPrimary = true, elementTypeParams = new { max_length = "256" } },
                    new { fieldName = "embedding", dataType = "FloatVector", elementTypeParams = new { dim = vectorSize.ToString() } },
                    new { fieldName = "data", dataType = "VarChar", elementTypeParams = new { max_length = "65535" } },
                    new { fieldName = "userId", dataType = "VarChar", elementTypeParams = new { max_length = "256" } },
                    new { fieldName = "agentId", dataType = "VarChar", elementTypeParams = new { max_length = "256" } },
                    new { fieldName = "runId", dataType = "VarChar", elementTypeParams = new { max_length = "256" } },
                    new { fieldName = "metadata", dataType = "VarChar", elementTypeParams = new { max_length = "65535" } },
                    new { fieldName = "createdAt", dataType = "VarChar", elementTypeParams = new { max_length = "64" } },
                    new { fieldName = "updatedAt", dataType = "VarChar", elementTypeParams = new { max_length = "64" } },
                    new { fieldName = "hash", dataType = "VarChar", elementTypeParams = new { max_length = "256" } }
                }
            },
            indexParams = new[]
            {
                new
                {
                    fieldName = "embedding",
                    indexName = "embedding_index",
                    metricType = "COSINE"
                }
            }
        };

        var response = await _httpClient.PostAsJsonAsync("/v2/vectordb/collections/create", createRequest, ct);
        await CheckResponseAsync(response);
    }

    public async Task InsertAsync(List<MemoryItem> memories, CancellationToken ct = default)
    {
        if (memories == null || memories.Count == 0)
        {
            return;
        }

        var data = memories.Select(m => new Dictionary<string, object>
        {
            ["id"] = m.Id,
            ["embedding"] = m.Embedding,
            ["data"] = m.Data,
            ["userId"] = m.UserId ?? string.Empty,
            ["agentId"] = m.AgentId ?? string.Empty,
            ["runId"] = m.RunId ?? string.Empty,
            ["metadata"] = System.Text.Json.JsonSerializer.Serialize(m.Metadata),
            ["createdAt"] = m.CreatedAt.ToString("o"),
            ["updatedAt"] = m.UpdatedAt?.ToString("o") ?? string.Empty,
            ["hash"] = m.Hash ?? string.Empty
        }).ToList();

        var request = new
        {
            collectionName = _collectionName,
            data
        };

        var response = await _httpClient.PostAsJsonAsync("/v2/vectordb/entities/insert", request, ct);
        // Parse response to verify insertion
        var result = await ReadResponseAsync<MilvusV2InsertResult>(response);
        // Verify insert count matches
        if (result?.InsertCount != memories.Count)
        {
            throw new InvalidOperationException(
                $"Insert count mismatch. Expected {memories.Count}, got {result?.InsertCount}");
        }
    }

    public async Task UpdateAsync(List<MemoryItem> memories, CancellationToken ct = default)
    {
        // Delete existing entries first
        var ids = memories.Select(m => m.Id).ToList();
        await DeleteMultipleAsync(ids, ct);

        // Insert updated memories
        await InsertAsync(memories, ct);
    }

    public async Task<List<MemorySearchResult>> SearchAsync(float[] queryVector, string? userId = null, int limit = 100, CancellationToken ct = default)
    {
        var searchRequest = new
        {
            collectionName = _collectionName,
            data = new[] { queryVector },
            annsField = "embedding",
            limit,
            outputFields = new[] { "id", "data", "userId", "agentId", "runId", "metadata", "createdAt", "updatedAt", "hash" },
            filter = userId != null ? $"userId == \"{userId}\"" : (string?)null,
        };

        var response = await _httpClient.PostAsJsonAsync("/v2/vectordb/entities/search", searchRequest, ct);
        var result = await ReadResponseAsync<MilvusV2SearchItem[]>(response);

        return result.Select(item => new MemorySearchResult
        {
            Id = item.Id,
            Memory = new MemoryItem
            {
                Id = item.Id,
                Data = item.Data,
                Embedding = Array.Empty<float>(),
                UserId = item.UserId,
                AgentId = item.AgentId,
                RunId = item.RunId,
                Metadata = !string.IsNullOrEmpty(item.Metadata) 
                    ? System.Text.Json.JsonSerializer.Deserialize<Dictionary<string, object>>(item.Metadata) ?? new Dictionary<string, object>()
                    : new Dictionary<string, object>(),
                CreatedAt = DateTime.Parse(item.CreatedAt),
                UpdatedAt = !string.IsNullOrEmpty(item.UpdatedAt) ? DateTime.Parse(item.UpdatedAt) : null,
                Hash = item.Hash
            },
            Score = item.Distance
        }).ToList();
    }

    public async Task<List<MemoryItem>> ListAsync(string? userId = null, int limit = 100, CancellationToken ct = default)
    {
        object queryRequest;
        if (userId != null)
        {
            queryRequest = new
            {
                collectionName = _collectionName,
                filter = $"userId == \"{userId}\"",
                limit,
                outputFields = new[] { "id", "data", "userId", "agentId", "runId", "metadata", "createdAt", "updatedAt", "hash" },
            };
        }
        else
        {
            // When no filter, omit the filter parameter entirely
            queryRequest = new
            {
                collectionName = _collectionName,
                limit,
                outputFields = new[] { "id", "data", "userId", "agentId", "runId", "metadata", "createdAt", "updatedAt", "hash" }
            };
        }

        var response = await _httpClient.PostAsJsonAsync("/v2/vectordb/entities/query", queryRequest, ct);
        var result = await ReadResponseAsync<MilvusV2QueryItem[]>(response);

        return result?.Select(item => new MemoryItem
        {
            Id = item.Id,
            Data = item.Data,
            Embedding = Array.Empty<float>(),
            UserId = item.UserId,
            AgentId = item.AgentId,
            RunId = item.RunId,
            Metadata = !string.IsNullOrEmpty(item.Metadata)
                ? System.Text.Json.JsonSerializer.Deserialize<Dictionary<string, object>>(item.Metadata) ?? new Dictionary<string, object>()
                : new Dictionary<string, object>(),
            CreatedAt = DateTime.Parse(item.CreatedAt),
            UpdatedAt = !string.IsNullOrEmpty(item.UpdatedAt) ? DateTime.Parse(item.UpdatedAt) : null,
            Hash = item.Hash
        }).ToList() ?? new List<MemoryItem>();
    }

    public async Task<MemoryItem?> GetAsync(string memoryId, CancellationToken ct = default)
    {
        var queryRequest = new
        {
            collectionName = _collectionName,
            filter = $"id == \"{memoryId}\"",
            limit = 1,
            outputFields = new[] { "id", "embedding", "data", "userId", "agentId", "runId", "metadata", "createdAt", "updatedAt", "hash" }
        };

        var response = await _httpClient.PostAsJsonAsync("/v2/vectordb/entities/query", queryRequest, ct);
        var result = await ReadResponseAsync<List<MilvusV2QueryItem>>(response);
        
        var item = result?.FirstOrDefault();
        if (item == null)
        {
            return null;
        }

        return new MemoryItem
        {
            Id = item.Id,
            Data = item.Data,
            Embedding = item.Embedding ?? Array.Empty<float>(),
            UserId = item.UserId,
            AgentId = item.AgentId,
            RunId = item.RunId,
            Metadata = !string.IsNullOrEmpty(item.Metadata)
                ? System.Text.Json.JsonSerializer.Deserialize<Dictionary<string, object>>(item.Metadata) ?? new Dictionary<string, object>()
                : new Dictionary<string, object>(),
            CreatedAt = DateTime.Parse(item.CreatedAt),
            UpdatedAt = !string.IsNullOrEmpty(item.UpdatedAt) ? DateTime.Parse(item.UpdatedAt) : null,
            Hash = item.Hash
        };
    }

    public async Task DeleteAsync(string memoryId, CancellationToken ct = default)
    {
        await DeleteMultipleAsync(new List<string> { memoryId }, ct);
    }

    public async Task DeleteByUserAsync(string userId, CancellationToken ct = default)
    {
        var deleteRequest = new
        {
            collectionName = _collectionName,
            filter = $"userId == \"{userId}\""
        };

        var response = await _httpClient.PostAsJsonAsync("/v2/vectordb/entities/delete", deleteRequest, ct);
        await CheckResponseAsync(response);
    }

    private async Task DeleteMultipleAsync(List<string> ids, CancellationToken ct = default)
    {
        var deleteRequest = new
        {
            collectionName = _collectionName,
            filter = $"id in [{string.Join(",", ids.Select(id => $"\"{id}\""))}]"
        };

        var response = await _httpClient.PostAsJsonAsync("/v2/vectordb/entities/delete", deleteRequest, ct);
        await CheckResponseAsync(response);
    }
    
    private async Task CheckResponseAsync(HttpResponseMessage response)
    {
        _ = await ReadResponseAsync<object>(response);
    }
    
    private static async Task<T> ReadResponseAsync<T>(HttpResponseMessage response)
    {
        var content = await response.Content.ReadAsStringAsync();
        if (!response.IsSuccessStatusCode)
        {
            throw new InvalidOperationException(
                $"Request failed with status code {response.StatusCode}. Response: {content}");
        }

        try
        {
            MilvusV2Response<T> responseObj = JsonSerializer.Deserialize<MilvusV2Response<T>>(content);
            if (responseObj == null || responseObj.Code != 0)
            {
                throw new InvalidOperationException(
                    $"Milvus returned error code {responseObj?.Code}: {responseObj?.Message}. Response: {content}");
            }

            return responseObj.Data!;
        }
        catch (JsonException e)
        {
            throw new InvalidOperationException($"Failed to parse response JSON. Response: {content}", e);
        }
    }

    // Internal classes for JSON deserialization
    private class MilvusV2Response<T>
    {
        [JsonPropertyName("code")]
        public int Code { get; set; }

        [JsonPropertyName("data")]
        public T? Data { get; set; }

        [JsonPropertyName("message")]
        public string? Message { get; set; }
    }

    private class MilvusV2CollectionInfo
    {
        [JsonPropertyName("collectionName")]
        public string CollectionName { get; set; } = string.Empty;

        [JsonPropertyName("fields")]
        public MilvusV2Field[] Fields { get; set; }
    }

    private class MilvusV2Field
    {
        [JsonPropertyName("name")]
        public string Name { get; set; } = string.Empty;

        [JsonPropertyName("type")]
        public string Type { get; set; } = string.Empty;

        [JsonPropertyName("params")]
        public MilvusV2FieldParam[] Params { get; set; }
    }
    
    private class MilvusV2FieldParam
    {
        [JsonPropertyName("key")]
        public string Key { get; set; }

        [JsonPropertyName("value")]
        public string Value { get; set; }
    }

    private class MilvusV2SearchItem
    {
        [JsonPropertyName("id")]
        public string Id { get; set; } = string.Empty;

        [JsonPropertyName("distance")]
        public float Distance { get; set; }

        [JsonPropertyName("data")]
        public string Data { get; set; } = string.Empty;

        [JsonPropertyName("userId")]
        public string? UserId { get; set; }

        [JsonPropertyName("agentId")]
        public string? AgentId { get; set; }

        [JsonPropertyName("runId")]
        public string? RunId { get; set; }

        [JsonPropertyName("metadata")]
        public string Metadata { get; set; } = string.Empty;

        [JsonPropertyName("createdAt")]
        public string CreatedAt { get; set; } = string.Empty;

        [JsonPropertyName("updatedAt")]
        public string? UpdatedAt { get; set; }

        [JsonPropertyName("hash")]
        public string? Hash { get; set; }
    }

    private class MilvusV2QueryItem
    {
        [JsonPropertyName("id")]
        public string Id { get; set; } = string.Empty;

        [JsonPropertyName("embedding")]
        public float[]? Embedding { get; set; }

        [JsonPropertyName("data")]
        public string Data { get; set; } = string.Empty;

        [JsonPropertyName("userId")]
        public string? UserId { get; set; }

        [JsonPropertyName("agentId")]
        public string? AgentId { get; set; }

        [JsonPropertyName("runId")]
        public string? RunId { get; set; }

        [JsonPropertyName("metadata")]
        public string Metadata { get; set; } = string.Empty;

        [JsonPropertyName("createdAt")]
        public string CreatedAt { get; set; } = string.Empty;

        [JsonPropertyName("updatedAt")]
        public string? UpdatedAt { get; set; }

        [JsonPropertyName("hash")]
        public string? Hash { get; set; }
    }

    private class MilvusV2InsertResult
    {
        [JsonPropertyName("insertCount")]
        public int InsertCount { get; set; }

        [JsonPropertyName("insertIds")]
        public List<string>? InsertIds { get; set; }
    }
}

