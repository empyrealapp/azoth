# azoth-vector

Vector similarity search extension for Azoth using [sqlite-vector](https://github.com/sqliteai/sqlite-vector).

## Features

- **Multiple vector types**: Float32, Float16, Int8, 1-bit
- **Multiple distance metrics**: L2, Cosine, Dot Product, Hamming
- **Fast k-NN search**: SIMD-optimized for modern CPUs
- **No preindexing**: Works immediately on insertion
- **Filtering support**: Combine vector search with SQL WHERE clauses
- **Migration helpers**: Easy table and column creation

## Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
azoth = { version = "0.1", features = ["vector"] }
azoth-vector = "0.1"
```

## Prerequisites

Download the sqlite-vector extension for your platform:

```bash
# Linux
wget https://github.com/sqliteai/sqlite-vector/releases/latest/download/libsqlite_vector-linux-x86_64.so

# macOS
wget https://github.com/sqliteai/sqlite-vector/releases/latest/download/libsqlite_vector-macos-universal.dylib

# Windows
wget https://github.com/sqliteai/sqlite-vector/releases/latest/download/sqlite_vector-windows-x86_64.dll
```

## Quick Start

```rust
use azoth::prelude::*;
use azoth_vector::{VectorExtension, Vector, VectorSearch, VectorConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize Azoth
    let db = AzothDb::open("./data")?;

    // Load vector extension
    db.projection().load_vector_extension(None)?;

    // Create table with vector column
    db.projection().execute(|conn| {
        conn.execute_batch(
            "CREATE TABLE embeddings (
                id INTEGER PRIMARY KEY,
                text TEXT NOT NULL,
                vector BLOB
            )"
        )?;
        Ok(())
    })?;

    // Initialize vector column (384-dimensional, cosine similarity)
    db.projection().vector_init(
        "embeddings",
        "vector",
        VectorConfig::default(),
    )?;

    // Insert vectors
    let vector = Vector::new(vec![0.1, 0.2, 0.3, /* ... 381 more values */]);
    db.projection().transaction(|txn| {
        txn.execute(
            "INSERT INTO embeddings (text, vector) VALUES (?, ?)",
            rusqlite::params!["Hello world", vector.to_blob()],
        )?;
        Ok(())
    })?;

    // Search for similar vectors
    let query = Vector::new(vec![0.15, 0.25, 0.35, /* ... */]);
    let search = VectorSearch::new(db.projection(), "embeddings", "vector");
    let results = search.knn(&query, 10).await?;

    for result in results {
        println!("Row {}: distance = {}", result.rowid, result.distance);
    }

    Ok(())
}
```

## Using with Migrations

```rust
use azoth::prelude::*;
use azoth_vector::{create_vector_table, VectorConfig, VectorType, DistanceMetric};

struct CreateEmbeddingsTable;

impl Migration for CreateEmbeddingsTable {
    fn version(&self) -> u32 { 2 }
    fn name(&self) -> &str { "create_embeddings_table" }

    fn up(&self, conn: &Connection) -> Result<()> {
        create_vector_table(
            conn,
            "embeddings",
            "id INTEGER PRIMARY KEY, text TEXT, vector BLOB, metadata TEXT",
            "vector",
            VectorConfig {
                vector_type: VectorType::Float32,
                dimension: 384,
                distance_metric: DistanceMetric::Cosine,
            },
        )?;

        // Create indexes
        conn.execute(
            "CREATE INDEX idx_embeddings_metadata ON embeddings(metadata)",
            [],
        )?;

        Ok(())
    }

    fn down(&self, conn: &Connection) -> Result<()> {
        conn.execute("DROP TABLE embeddings", [])?;
        Ok(())
    }
}

// Apply migration
let mut mgr = MigrationManager::new();
mgr.add(Box::new(CreateEmbeddingsTable));
mgr.run(db.projection())?;
```

## Advanced Search with Filtering

```rust
use azoth_vector::{VectorSearch, DistanceMetric};

let query = Vector::new(query_embedding);

// Search with custom distance metric
let search = VectorSearch::new(db.projection(), "embeddings", "vector")
    .distance_metric(DistanceMetric::L2);

// k-NN search
let results = search.knn(&query, 20).await?;

// Search with distance threshold
let similar_only = search.threshold(&query, 0.5, 100).await?;

// Search with structured filter
let filter = VectorFilter::new()
    .like("metadata", "%important%")
    .gt("created_at", "2024-01-01");

let filtered = search
    .knn_filtered(&query, 10, &filter)
    .await?;
```

## Vector Types

```rust
use azoth_vector::{VectorType, VectorConfig};

// Float32 (default) - best accuracy
VectorConfig {
    vector_type: VectorType::Float32,
    dimension: 384,
    ..Default::default()
}

// Float16 - half size, slight accuracy loss
VectorConfig {
    vector_type: VectorType::Float16,
    dimension: 768,
    ..Default::default()
}

// Int8 - quarter size, quantized
VectorConfig {
    vector_type: VectorType::Int8,
    dimension: 1536,
    ..Default::default()
}

// 1-bit - 32x smaller, binary embeddings
VectorConfig {
    vector_type: VectorType::Bit1,
    dimension: 1024,
    ..Default::default()
}
```

## Distance Metrics

- **L2** (Euclidean): Standard geometric distance
- **SquaredL2**: Faster than L2, same ranking
- **L1** (Manhattan): Sum of absolute differences
- **Cosine**: Angle between vectors (normalized)
- **DotProduct**: Inner product (for normalized vectors)
- **Hamming**: Bit differences (for 1-bit vectors only)

## Event-Sourced Vector Updates

Combine with Azoth's event sourcing:

```rust
#[derive(Serialize, Deserialize)]
enum DocumentEvent {
    Added { id: i64, text: String, embedding: Vec<f32> },
    Updated { id: i64, new_embedding: Vec<f32> },
    Deleted { id: i64 },
}

impl EventApplier for DocumentApplier {
    fn apply(&self, event: &Event, txn: &Transaction) -> Result<()> {
        let doc_event: DocumentEvent = serde_json::from_value(event.data.clone())?;

        match doc_event {
            DocumentEvent::Added { id, text, embedding } => {
                let vector = Vector::new(embedding);
                txn.execute(
                    "INSERT INTO documents (id, text, vector) VALUES (?, ?, ?)",
                    params![id, text, vector.to_blob()],
                )?;
            }
            DocumentEvent::Updated { id, new_embedding } => {
                let vector = Vector::new(new_embedding);
                txn.execute(
                    "UPDATE documents SET vector = ? WHERE id = ?",
                    params![vector.to_blob(), id],
                )?;
            }
            DocumentEvent::Deleted { id } => {
                txn.execute("DELETE FROM documents WHERE id = ?", params![id])?;
            }
        }

        Ok(())
    }
}
```

## Performance

- ~10-20ms search latency for 10k vectors (Float32, 384-dim)
- ~30MB RAM overhead (default)
- SIMD-optimized for modern CPUs
- Zero-cost updates (no index rebuilding)

## Example Use Cases

### RAG (Retrieval Augmented Generation)

```rust
// Store document chunks with embeddings
let chunks = process_document(pdf_path)?;
for chunk in chunks {
    let embedding = get_embedding(&chunk.text).await?;
    let vector = Vector::new(embedding);

    db.projection().transaction(|txn| {
        txn.execute(
            "INSERT INTO knowledge (text, source, vector) VALUES (?, ?, ?)",
            params![chunk.text, chunk.source, vector.to_blob()],
        )?;
        Ok(())
    })?;
}

// Search for relevant context
let query_embedding = get_embedding(user_query).await?;
let search = VectorSearch::new(db.projection(), "knowledge", "vector");
let results = search.knn(&Vector::new(query_embedding), 5).await?;
```

### Recommendation System

```rust
// Find similar items
let user_preferences = Vector::new(user_embedding);
let search = VectorSearch::new(db.projection(), "items", "embedding");

let filter = VectorFilter::new()
    .eq("category", &user_category)
    .eq_i64("in_stock", 1);

let recommendations = search
    .knn_filtered(&user_preferences, 20, &filter)
    .await?;
```

### Semantic Deduplication

```rust
// Check if similar content exists
let new_content_embedding = get_embedding(content).await?;
let search = VectorSearch::new(db.projection(), "content", "embedding");

let duplicates = search
    .threshold(&Vector::new(new_content_embedding), 0.1, 5)
    .await?;

if duplicates.is_empty() {
    // Add new unique content
} else {
    // Mark as duplicate
}
```

## License

MIT OR Apache-2.0
