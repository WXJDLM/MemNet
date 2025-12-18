using System;
using HttpMataki.NET.Auto;
using MemNet;
using MemNet.Abstractions;
using MemNet.Models;
using MemNet.Redis;
using MemNet.VectorStores;
using Microsoft.Extensions.DependencyInjection;

//HttpClientAutoInterceptor.StartInterception();

var services = new ServiceCollection();
services.AddMemNet(config =>
{
    config.Embedder.Endpoint = "https://ark.cn-beijing.volces.com/api/v3/";
    config.Embedder.Model = "doubao-embedding-text-240715";
    config.Embedder.ApiKey = "37ca8174-cde5-4eb2-8d97-e4d299cc89d8";

    config.LLM.Endpoint = "https://ark.cn-beijing.volces.com/api/v3/";
    config.LLM.Model = "doubao-1-5-pro-32k-250115";
    config.LLM.ApiKey = "37ca8174-cde5-4eb2-8d97-e4d299cc89d8";

    //config.VectorStore.Endpoint = "http://localhost:6333";//Qdrant
    //config.VectorStore.Endpoint = "http://localhost:19530";//Milvus
    //config.VectorStore.CollectionName = "c3";
    config.VectorStore.Endpoint = "http://localhost:8000";
}).WithMemNetRedis("110.41.143.33:6379");//.WithMilvusV2();//.WithQdrant();
/*
.WithChromaV2();

services.Configure<ChromaV2VectorStoreConfig>(e =>
{
e.Endpoint = "http://localhost:8000";
e.Database = "default";
e.Tenant = "default";
});*/

await using var sp = services.BuildServiceProvider();
var memoryService = sp.GetRequiredService<IMemoryService>();
await memoryService.InitializeAsync(true);

//await memoryService.DeleteAllAsync("user001");
await memoryService.AddAsync(new AddMemoryRequest
{
    Messages =
    [
        new MessageContent
        {
            Role = "User",
            Content = "My name is Zack. I love programming."
        },
        new MessageContent
        {
            Role = "User",
            Content = "As a 18-years-old boy, I'm into Chinese food."
        },
        new MessageContent
        {
            Role = "User",
            Content = "I'm 20 years old."
        },
        new MessageContent
        {
            Role = "User",
            Content = "I'm allergic to nuts."
        }
    ],
    UserId = "user001"
});

/*
Console.WriteLine("All memories");
foreach (var item in await memoryService.GetAllAsync( "user001"))
{
    Console.WriteLine($"- {item.Data}");
}
*/
Console.WriteLine("Search Results:");

var resp = await memoryService.SearchAsync(new SearchMemoryRequest
{
    Query = "Please recommend some food.", //"Am I old?",
    UserId = "user001"
});
foreach (var item in resp.ToArray())
{
    Console.WriteLine($"- {item.Memory.Data}");
}