---
title: Getting Started
description: How to install and run klite.
---

# Getting Started

klite is a Kafka-compatible broker in a single Go binary.

## Installation

Download the latest release for your platform:

```bash
# macOS (Apple Silicon)
curl -L https://github.com/kliteio/klite/releases/latest/download/klite-darwin-arm64 -o klite

# Linux (amd64)
curl -L https://github.com/kliteio/klite/releases/latest/download/klite-linux-amd64 -o klite

chmod +x klite
```

## Quick Start

Start the broker:

```bash
./klite
```

klite will start listening on port `9092` by default. You can now connect any Kafka client to `localhost:9092`.

## Produce and Consume

Use any Kafka client. For example, with `kcat`:

```bash
# Produce a message
echo "hello klite" | kcat -P -b localhost:9092 -t my-topic

# Consume messages
kcat -C -b localhost:9092 -t my-topic -e
```
