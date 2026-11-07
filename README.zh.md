# Weaviate <img alt='Weaviate logo' src='https://weaviate.io/img/site/weaviate-logo-light.png' width='148' align='right' />

[![GitHub Repo stars](https://img.shields.io/github/stars/weaviate/weaviate?style=social)](https://github.com/weaviate/weaviate)
[![Go Reference](https://pkg.go.dev/badge/github.com/weaviate/weaviate.svg)](https://pkg.go.dev/github.com/weaviate/weaviate)
[![Build Status](https://github.com/weaviate/weaviate/actions/workflows/.github/workflows/pull_requests.yaml/badge.svg?branch=main)](https://github.com/weaviate/weaviate/actions/workflows/.github/workflows/pull_requests.yaml)
[![Go Report Card](https://goreportcard.com/badge/github.com/weaviate/weaviate)](https://goreportcard.com/report/github.com/weaviate/weaviate)
[![Coverage Status](https://codecov.io/gh/weaviate/weaviate/branch/main/graph/badge.svg)](https://codecov.io/gh/weaviate/weaviate)

<p align="center">
    <a href="README.md">English</a> · <b>简体中文</b>
</p>

**Weaviate** 是一款开源、云原生的向量数据库，能够同时存储数据对象与向量，支持海量规模的高性能语义搜索。它将向量相似度搜索与关键词过滤、检索增强生成（RAG）以及重排序（Reranking）深度整合在统一的查询接口中。常见应用场景包括 RAG 系统、语义搜索、以图搜图、推荐引擎、AI 智能体与对话机器人以及内容分类。

Weaviate 支持两种向量存储与生成方式：在数据导入时使用[内置集成模型](https://docs.weaviate.io/weaviate/model-providers)（支持 OpenAI、Cohere、HuggingFace 等）自动向量化，或直接导入[预计算的向量嵌入](https://docs.weaviate.io/weaviate/starter-guides/custom-vectors)。生产级部署更受益于原生内置的多租户隔离、多副本机制、基于角色的访问控制（RBAC）授权等[诸多企业级特性](#weaviate-核心特性)。

如需快速上手，请参阅以下入门指南：

- [快速入门 - Weaviate Cloud 全托管云服务](https://docs.weaviate.io/weaviate/quickstart)
- [快速入门 - 本地 Docker 实例部署](https://docs.weaviate.io/weaviate/quickstart/local)

## 安装部署

Weaviate 提供多种灵活的安装与部署选项：

- [Docker 部署](https://docs.weaviate.io/deploy/installation-guides/docker-installation)
- [Kubernetes 部署](https://docs.weaviate.io/deploy/installation-guides/k8s-installation)
- [Weaviate Cloud 云服务](https://console.weaviate.cloud)

更多部署方案（如 [AWS Marketplace](https://docs.weaviate.io/deploy/installation-guides/aws-marketplace) 与 [GCP Marketplace](https://docs.weaviate.io/deploy/installation-guides/gcp-marketplace)），请参阅[官方安装部署文档](https://docs.weaviate.io/deploy)。

## 快速上手

你可以借助 [Docker](https://docs.docker.com/desktop/) 快速启动 Weaviate 以及本地轻量向量嵌入模型服务。
首先创建 `docker-compose.yml` 文件：

```yml
services:
  weaviate:
    image: cr.weaviate.io/semitechnologies/weaviate:1.36.0
    ports:
      - "8080:8080"
      - "50051:50051"
    environment:
      ENABLE_MODULES: text2vec-model2vec
      MODEL2VEC_INFERENCE_API: http://text2vec-model2vec:8080

  # 轻量级向量嵌入模型服务，在数据导入时自动为对象生成向量
  text2vec-model2vec:
    image: cr.weaviate.io/semitechnologies/model2vec-inference:minishlab-potion-base-32M
```

使用以下命令启动 Weaviate 与向量模型服务：

```bash
docker compose up -d
```

安装 Python 官方客户端（或使用其他语言的[客户端库](#客户端库与-api)）：

```bash
pip install -U weaviate-client
```

以下 Python 示例演示了如何向 Weaviate 写入数据、自动生成向量嵌入并执行语义搜索：

```python
import weaviate
from weaviate.classes.config import Configure, DataType, Property

# 连接到本地 Weaviate 实例
client = weaviate.connect_to_local()

# 创建集合 (Collection)
client.collections.create(
    name="Article",
    properties=[Property(name="content", data_type=DataType.TEXT)],
    vector_config=Configure.Vectors.text2vec_model2vec(),  # 导入时使用向量化器自动生成向量嵌入
    # vector_config=Configure.Vectors.self_provided()  # 若需要导入预先计算好的向量，请取消此行注释
)

# 插入数据对象并自动生成向量嵌入
articles = client.collections.get("Article")
articles.data.insert_many(
    [
        {"content": "Vector databases enable semantic search"},
        {"content": "Machine learning models generate embeddings"},
        {"content": "Weaviate supports hybrid search capabilities"},
    ]
)

# 执行基于语义的近邻查询 (near_text)
results = articles.query.near_text(query="Search objects by meaning", limit=1)
print(results.objects[0])

client.close()
```

此示例使用了 `Model2Vec` 向量化器，你也可以根据需要切换至其他[嵌入模型提供商](https://docs.weaviate.io/weaviate/model-providers)或[自带预计算向量](https://docs.weaviate.io/weaviate/starter-guides/custom-vectors)。

## 客户端库与 API

Weaviate 针对多种编程语言提供官方 SDK 客户端：

- [Python](https://docs.weaviate.io/weaviate/client-libraries/python)
- [JavaScript/TypeScript](https://docs.weaviate.io/weaviate/client-libraries/typescript)
- [Java](https://docs.weaviate.io/weaviate/client-libraries/java)
- [Go](https://docs.weaviate.io/weaviate/client-libraries/go)
- [C#/.NET](https://docs.weaviate.io/weaviate/client-libraries/csharp)

此外，社区还维护了丰富的[第三方语言库](https://docs.weaviate.io/weaviate/client-libraries/community)。

Weaviate 对外提供 [REST API](https://docs.weaviate.io/weaviate/api/rest)、[gRPC API](https://docs.weaviate.io/weaviate/api/grpc) 与 [GraphQL API](https://docs.weaviate.io/weaviate/api/graphql) 接口与数据库服务端高效通信。

## Weaviate 核心特性

以下关键特性助力构建强大的现代化 AI 应用程序：

- **⚡ 极致检索性能**：毫秒级在数十亿级向量上执行复杂语义[搜索](https://docs.weaviate.io/weaviate/search/similarity)。Weaviate 底层基于 Go 语言构建，兼顾高吞吐与高可靠性，确保在高负载下依然具备极佳响应速度。详见 [ANN 基准测试](https://docs.weaviate.io/weaviate/benchmarks/ann)。

- **🔌 灵活的向量化支持**：导入时无缝结合来自 OpenAI、Cohere、HuggingFace、Google 等厂商的[内置向量化器](https://docs.weaviate.io/weaviate/model-providers)，亦可直接导入[自带预计算向量嵌入](https://docs.weaviate.io/weaviate/starter-guides/custom-vectors)。

- **🔍 先进的混合检索与以图搜图**：在单次 API 调用中融合语义搜索、经典 [BM25 关键字检索](https://docs.weaviate.io/weaviate/search/bm25)、[图像搜索](https://docs.weaviate.io/weaviate/search/image)以及[高级过滤条件](https://docs.weaviate.io/weaviate/search/filters)，实现兼具精准度与召回率的最优检索结果。

- **🤖 内置 RAG 与重排序能力**：超越普通检索，原生提供[生成式搜索（RAG）](https://docs.weaviate.io/weaviate/search/generative)与[重排序 (Reranking)](https://docs.weaviate.io/weaviate/search/rerank)能力。无需引入繁杂外部工具，直接在数据库层驱动问答系统、智能体对话与摘要提炼。

- **📈 生产就绪与水平弹性扩展**：专为企业级关键任务系统打造。支持从快速原型无缝过渡到大规模生产环境，原生具备[水平横向扩展](https://docs.weaviate.io/deploy/configuration/horizontal-scaling)、[多租户隔离](https://docs.weaviate.io/weaviate/manage-collections/multi-tenancy)、[数据多副本](https://docs.weaviate.io/deploy/configuration/replication)以及细粒度[基于角色的访问控制（RBAC）](https://docs.weaviate.io/weaviate/configuration/rbac)。

- **💰 成本效益与资源优化**：利用内置[向量压缩技术](https://docs.weaviate.io/weaviate/configuration/compression)大幅降低物理内存占用与运营成本。向量量化（Quantization）与多向量编码在保障搜索精度的同时大幅减轻资源负担。

- **⏱️ 对象生命周期管理 (TTL)**：支持按集合配置灵活的[存活时间 (Time-To-Live)](https://docs.weaviate.io/weaviate/manage-collections/time-to-live)，自动清理过期陈旧数据，并完全兼容 RBAC 与多租户环境。

完整功能特性清单请访问 [Weaviate 官方文档](https://docs.weaviate.io)。

## 实用资源

### AI Agent Skills

[Weaviate Agent Skills](https://github.com/weaviate/agent-skills) 是一套为 AI 编码助手（Claude Code、Cursor、GitHub Copilot 等）量身打造的技能集，帮助智能体更准确、高效地使用 Weaviate。涵盖语义搜索、复杂查询、集合管理、数据导入以及完整应用蓝图（RAG、Agentic RAG、对话机器人等）。

一键安装命令：

```bash
npx skills add weaviate/agent-skills
```

### 演示项目与实践配方 (Demo Projects & Recipes)

这些演示项目均为主流开源真实应用，源码均已在 GitHub 开源：

- [Elysia](https://elysia.weaviate.io) ([GitHub](https://github.com/weaviate/elysia))：基于决策树的智能体系统，可智能评估何时调用工具、如何利用中间结果以及目标是否达成。
- [Verba](https://weaviate.io/blog/verba-open-source-rag-app) ([GitHub](https://github.com/weaviate/verba))：社区驱动的开源端到端 RAG 应用，开箱即用提供极具友好度的界面与完备功能。
- [Healthsearch](https://weaviate.io/blog/healthsearch-demo) ([GitHub](https://github.com/weaviate/healthsearch-demo))：展示如何根据用户真实评论与搜索意图，依据特定健康功效检索营养补剂产品。
- Awesome-Moviate ([GitHub](https://github.com/weaviate-tutorials/awesome-moviate))：支持关键词 (BM25)、语义与混合检索的电影推荐与搜索引擎。

我们同时维护了涵盖 Weaviate 各项特性与集成的 **Jupyter Notebooks** 与 **TypeScript 代码示例** 仓库：

- [Weaviate Python Recipes 实践配方](https://github.com/weaviate/recipes/)
- [Weaviate TypeScript Recipes 实践配方](https://github.com/weaviate/recipes-ts/)

### 精选技术博客

- [什么是向量数据库 (What is a Vector Database)](https://weaviate.io/blog/what-is-a-vector-database)
- [图解向量搜索 (What is Vector Search)](https://weaviate.io/blog/vector-search-explained)
- [图解混合检索 (What is Hybrid Search)](https://weaviate.io/blog/hybrid-search-explained)
- [如何挑选最适合的嵌入模型 (How to Choose an Embedding Model)](https://weaviate.io/blog/how-to-choose-an-embedding-model)
- [RAG 检索增强生成入门介绍 (What is RAG)](https://weaviate.io/blog/introduction-to-rag)
- [RAG 评估方法与框架 (RAG Evaluation)](https://weaviate.io/blog/rag-evaluation)
- [高级进阶 RAG 技术 (Advanced RAG Techniques)](https://weaviate.io/blog/advanced-rag)
- [多模态 RAG 解析 (What is Multimodal RAG)](https://weaviate.io/blog/multimodal-rag)
- [Agentic RAG 智能体检索增强架构 (What is Agentic RAG)](https://weaviate.io/blog/what-is-agentic-rag)
- [知识图谱 RAG (Graph RAG)](https://weaviate.io/blog/graph-rag)
- [后期交互模型概述 (Overview of Late Interaction Models)](https://weaviate.io/blog/late-interaction-overview)

### 生态集成

Weaviate 与业界主流外部服务紧密集成：

| 类别 | 描述 | 集成生态 |
| --- | --- | --- |
| **[云巨头服务 (Cloud Hyperscalers)](https://docs.weaviate.io/integrations/cloud-hyperscalers)** | 大规模计算与云原生存储 | [AWS](https://docs.weaviate.io/integrations/cloud-hyperscalers/aws), [Google](https://docs.weaviate.io/integrations/cloud-hyperscalers/google) |
| **[计算基础设施 (Compute Infrastructure)](https://docs.weaviate.io/integrations/compute-infrastructure)** | 容器化应用运行与弹性扩展 | [Modal](https://docs.weaviate.io/integrations/compute-infrastructure/modal), [Replicate](https://docs.weaviate.io/integrations/compute-infrastructure/replicate), [Replicated](https://docs.weaviate.io/integrations/compute-infrastructure/replicated) |
| **[数据平台 (Data Platforms)](https://docs.weaviate.io/integrations/data-platforms)** | 数据摄取、同步与网页采集抓取 | [Airbyte](https://docs.weaviate.io/integrations/data-platforms/airbyte), [Aryn](https://docs.weaviate.io/integrations/data-platforms/aryn), [Boomi](https://docs.weaviate.io/integrations/data-platforms/boomi), [Box](https://docs.weaviate.io/integrations/data-platforms/box), [Confluent](https://docs.weaviate.io/integrations/data-platforms/confluent), [Astronomer](https://docs.weaviate.io/integrations/data-platforms/astronomer), [Context Data](https://docs.weaviate.io/integrations/data-platforms/context-data), [Databricks](https://docs.weaviate.io/integrations/data-platforms/databricks), [Firecrawl](https://docs.weaviate.io/integrations/data-platforms/firecrawl), [IBM](https://docs.weaviate.io/integrations/data-platforms/ibm), [Unstructured](https://docs.weaviate.io/integrations/data-platforms/unstructured) |
| **[LLM 与 Agent 框架](https://docs.weaviate.io/integrations/llm-agent-frameworks)** | 构建智能体与生成式 AI 应用 | [Agno](https://docs.weaviate.io/integrations/llm-agent-frameworks/agno), [Composio](https://docs.weaviate.io/integrations/llm-agent-frameworks/composio), [CrewAI](https://docs.weaviate.io/integrations/llm-agent-frameworks/crewai), [DSPy](https://docs.weaviate.io/integrations/llm-agent-frameworks/dspy), [Dynamiq](https://docs.weaviate.io/integrations/llm-agent-frameworks/dynamiq), [Haystack](https://docs.weaviate.io/integrations/llm-agent-frameworks/haystack), [LangChain](https://docs.weaviate.io/integrations/llm-agent-frameworks/langchain), [LlamaIndex](https://docs.weaviate.io/integrations/llm-agent-frameworks/llamaindex), [N8n](https://docs.weaviate.io/integrations/llm-agent-frameworks/n8n), [Semantic Kernel](https://docs.weaviate.io/integrations/llm-agent-frameworks/semantic-kernel) |
| **[可观测性与运维 (Operations)](https://docs.weaviate.io/integrations/operations)** | 监控与评估生成式 AI 全链路工作流 | [AIMon](https://docs.weaviate.io/integrations/operations/aimon), [Arize](https://docs.weaviate.io/integrations/operations/arize), [Cleanlab](https://docs.weaviate.io/integrations/operations/cleanlab), [Comet](https://docs.weaviate.io/integrations/operations/comet), [DeepEval](https://docs.weaviate.io/integrations/operations/deepeval), [Langtrace](https://docs.weaviate.io/integrations/operations/langtrace), [LangWatch](https://docs.weaviate.io/integrations/operations/langwatch), [Nomic](https://docs.weaviate.io/integrations/operations/nomic), [Patronus AI](https://docs.weaviate.io/integrations/operations/patronus), [Ragas](https://docs.weaviate.io/integrations/operations/ragas), [TruLens](https://docs.weaviate.io/integrations/operations/trulens), [Weights & Biases](https://docs.weaviate.io/integrations/operations/wandb) |

## 参与贡献

我们非常欢迎来自社区的贡献！请参阅我们的[贡献者指南 (Contributor guide)](https://docs.weaviate.io/contributor-guide) 了解开发环境搭建、代码规范、测试要求与 PR 流程。

欢迎加入我们的 [社区论坛 (Community forum)](https://forum.weaviate.io/) 交流想法并获取支持帮助。

## 开源协议

Weaviate 采用 BSD 3-Clause 开源许可证。详见 [LICENSE](./LICENSE)。

---

> 💡 **文档维护说明**：本中文文档由社区志愿者（[@JasonYeYuhe](https://github.com/JasonYeYuhe)）翻译维护，最后同步更新于 2026年9月6日。如发现内容与官方英文原版存在差异或新特性滞后，欢迎提交 PR 共同完善！
