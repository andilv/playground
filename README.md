# Deno HTTPS Proxy Server

This project implements a simple HTTPS proxy server using Deno and TypeScript.

## Getting Started

### Prerequisites

- Deno (https://deno.land/)

### Running the proxy server

To start the proxy server, run the following command:

```bash
deno run --allow-net --allow-read main.ts
```

By default, the server will listen on HTTP port 8000 and HTTPS port 8443. You can configure the ports and other options in `deno.json`.

## Usage

Once the proxy server is running, you can configure your applications or system to use it as an HTTPS proxy.

