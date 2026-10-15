# Measurement artifacts

Dated JSON outputs of `examples/benchmark.py`. `meta.tessera_backend` in
each file names the object backend measured: `fs` (LocalBackend —
coordination overhead only), `endpoint` (S3DynamoBackend vs an
S3-compatible endpoint such as ministack — emulated, not production S3
latency), or `aws` (real AWS, pre-provisioned resources).

Files are committed so every number on the measurements page resolves to
a reproducible artifact. CI (`bench.yml`) adds `bench-micro-*` artifacts
per run; promote the interesting ones here with a dated filename.
