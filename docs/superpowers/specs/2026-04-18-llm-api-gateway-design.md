# LLM API Gateway Design

## Overview

Extend the existing `custom_plugin_llm_gateway` host plugin to expose HTTP API endpoints for LLM services. External clients can call OpenAI-compatible `/v1/chat/completions` and Anthropic-compatible `/v1/responses` endpoints, regardless of which LLM provider is configured on the backend. The `byokey-translate` crate handles API format translation between external formats and the internal genai provider layer.

## Goals

- Expose `/v1/chat/completions` (OpenAI Chat Completions format) HTTP endpoint
- Expose `/v1/responses` (Anthropic Responses format) HTTP endpoint
- Support SSE streaming for both endpoints
- Any genai-supported provider can serve both API formats (e.g., OpenAI backend can serve Anthropic Responses format)
- Preserve existing `chat`/`chat-streaming` WASI interfaces for Wasm components
- No changes to the existing WASI interface surface

## Non-Goals

- Adding new LLM providers beyond what genai already supports
- OAuth authentication flows (BYOKEY's auth features are not used)
- Token persistence or management
- Rate limiting or API key management at the gateway level

## Architecture

```
External HTTP Clients
    |
    | POST /v1/chat/completions  (OpenAI format)
    | POST /v1/responses         (Anthropic format)
    | SSE streaming support
    |
    v
Host HTTP Server -> routes to custom_plugin_llm_gateway
    |
    v
custom_plugin_llm_gateway (host plugin)
    |
    | +- wasi:http/incoming-handler (new)
    |    |- POST /v1/chat/completions
    |    |   -> byokey-translate parses OpenAI format
    |    |   -> calls genai client
    |    |   -> byokey-translate converts response to completions format
    |    |   -> SSE streaming return
    |    |
    |    |- POST /v1/responses
    |        -> byokey-translate parses Anthropic format
    |        -> calls genai client
    |        -> byokey-translate converts response to responses format
    |        -> SSE streaming return
    |
    | +- byokey-translate (API format translation layer)
    | +- byokey-types (data types)
    |
    | +- genai (provider calls, existing)
    |
    | +- Existing WASI chat/chat-streaming interfaces (unchanged)
```

### Key Design Decisions

1. **Plugin implements wasi:http/incoming-handler**: The plugin registers itself as an HTTP handler through the host's existing HTTP routing mechanism. No host routing code changes are needed.

2. **byokey-translate for format conversion**: The `byokey-translate` and `byokey-types` crates from the BYOKEY project provide the core translation logic. They handle:
   - OpenAI Chat Completions request parsing and response serialization
   - Anthropic Messages/Responses request parsing and response serialization
   - SSE streaming event translation for both formats

3. **genai remains the provider layer**: No changes to how the plugin calls LLM providers. genai handles OpenAI, Anthropic, Gemini, DeepSeek, Ollama, Groq, and OpenAI-compatible providers.

4. **Format-agnostic provider**: Any provider can serve any format. For example, a DeepSeek backend can respond to both OpenAI completions and Anthropic responses requests.

5. **Existing WASI interfaces unchanged**: Wasm components that import `custom:llm-gateway/chat` continue to work exactly as before.

## New Dependencies

Add to `crates/custom_plugin_llm_gateway/Cargo.toml`:

```toml
[dependencies]
byokey-translate = "1.0.0"
byokey-types = "1.0.0"
```

## Files to Modify

### 1. `crates/custom_plugin_llm_gateway/src/lib.rs`

Add HTTP handler module that:

- Implements request routing for `/v1/chat/completions` and `/v1/responses`
- Parses incoming JSON request bodies into byokey-types data structures
- Converts byokey-types requests to genai chat request format
- Calls the existing genai client (reusing the client pool)
- Converts genai chat responses back to the appropriate output format
- Handles SSE streaming by converting genai streaming events to the correct SSE format

### 2. `crates/custom_plugin_llm_gateway/Cargo.toml`

Add `byokey-translate` and `byokey-types` dependencies.

## API Endpoint Details

### POST /v1/chat/completions

OpenAI Chat Completions compatible endpoint.

**Request body** (OpenAI format):
```json
{
  "model": "gpt-4o-mini",
  "messages": [
    {"role": "system", "content": "You are a helpful assistant"},
    {"role": "user", "content": "Hello"}
  ],
  "temperature": 0.7,
  "max_tokens": 4096,
  "stream": false
}
```

**Response** (OpenAI format):
```json
{
  "id": "chatcmpl-xxx",
  "object": "chat.completion",
  "model": "gpt-4o-mini",
  "choices": [
    {
      "index": 0,
      "message": {"role": "assistant", "content": "Hello!"},
      "finish_reason": "stop"
    }
  ],
  "usage": {"prompt_tokens": 10, "completion_tokens": 5, "total_tokens": 15}
}
```

**Streaming**: When `"stream": true`, returns SSE events with `chat.completion.chunk` objects.

### POST /v1/responses

Anthropic Messages/Responses compatible endpoint.

**Request body** (Anthropic format):
```json
{
  "model": "claude-sonnet-4-5-20250514",
  "messages": [
    {"role": "user", "content": "Hello"}
  ],
  "system": "You are a helpful assistant",
  "temperature": 0.7,
  "max_tokens": 4096,
  "stream": false
}
```

**Response** (Anthropic format):
```json
{
  "id": "msg-xxx",
  "type": "message",
  "role": "assistant",
  "content": [
    {"type": "text", "text": "Hello!"}
  ],
  "model": "claude-sonnet-4-5-20250514",
  "stop_reason": "end_turn",
  "usage": {"input_tokens": 10, "output_tokens": 5}
}
```

**Streaming**: When `"stream": true`, returns SSE events with Anthropic event types (`message_start`, `content_block_delta`, `content_block_stop`, `message_stop`).

## Format Translation Flow

### Completions Flow (any provider -> OpenAI format)

1. Receive HTTP POST with OpenAI Chat Completions JSON body
2. byokey-translate: Parse `ChatCompletionRequest` from JSON
3. Convert to internal genai chat request format (model, messages, options)
4. Call genai client with the converted request
5. Convert genai response to `ChatCompletionResponse` via byokey-translate
6. Serialize to JSON and return HTTP response
7. For streaming: Convert genai stream events to `chat.completion.chunk` SSE events

### Responses Flow (any provider -> Anthropic format)

1. Receive HTTP POST with Anthropic Messages JSON body
2. byokey-translate: Parse Anthropic request from JSON
3. Convert to internal genai chat request format (model, messages, options, system prompt extraction)
4. Call genai client with the converted request
5. Convert genai response to Anthropic response format via byokey-translate
6. Serialize to JSON and return HTTP response
7. For streaming: Convert genai stream events to Anthropic SSE event types

## Configuration

No new configuration keys needed. The existing `custom:llm-gateway` config is used:

```yaml
host_interfaces:
  - namespace: custom
    package: llm-gateway
    interfaces:
      - chat
    config:
      provider: openai-compat
      base_url: https://openrouter.ai/api/v1/
      model_name: z-ai/glm-4.5-air:free
      api_key: sk-or-v1-xxx
```

The HTTP endpoints use the same provider configuration. The `model` field in the request body can override the default configured model.

## Error Handling

Errors are returned in the format appropriate to the endpoint:

- **Completions errors**: OpenAI-compatible error format
  ```json
  {
    "error": {
      "message": "error description",
      "type": "invalid_request_error",
      "code": null
    }
  }
  ```

- **Responses errors**: Anthropic-compatible error format
  ```json
  {
    "type": "error",
    "error": {
      "type": "invalid_request_error",
      "message": "error description"
    }
  }
  ```

HTTP status codes follow the respective API conventions (400 for bad request, 401 for auth errors, 429 for rate limits, 500 for server errors).

## Testing

1. **Unit tests**: Format translation logic (byokey-translate conversions)
2. **Integration tests**: Full HTTP request/response cycle for both endpoints
3. **Streaming tests**: SSE event sequence verification for both formats
4. **Cross-provider tests**: Verify that each provider can serve both API formats
5. **Backward compatibility tests**: Verify existing WASI chat/chat-streaming interfaces still work
