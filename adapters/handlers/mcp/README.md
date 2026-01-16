# Weaviate MCP Server Implementation

This directory contains the implementation of the Weaviate Model Context Protocol (MCP) server. The server is built using the [mcp-go.dev/servers](https://mcp-go.dev/servers) library, which provides a robust framework for exposing tools and data to Large Language Model (LLM) applications.

## Implemented Tools

The Weaviate MCP server currently implements the following tools:

*   **get-schema**: Retrieves the schema of the database.
*   **get-tenants**: Retrieves the tenants of a collection in the database.
*   **insert-one**: Inserts a single object into the database.
*   **search-with-hybrid**: Performs a hybrid search (combining keyword and vector search) on the database.
