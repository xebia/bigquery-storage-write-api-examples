alias i := install
alias p := pre_commit
alias s := setup
alias t := test
alias l := login
alias dse := default_stream_example
alias pte := pending_type_stream_example
alias bi := bq_init
alias gp := generate_proto
alias cp := compile_proto

# Install python dependencies
install:
  uv sync

# Install pre-commit hooks
pre_commit_setup:
  uv run pre-commit install

# Install python dependencies and pre-commit hooks
setup: install pre_commit_setup

# Run pre-commit
pre_commit:
 uv run pre-commit run -a

# Login to gcloud
login:
  gcloud auth application-default login

# Run pytest
test:
  uv run pytest tests

# Generate a proto file from a bigquery table schema
generate_proto entity:
  uv run examples generate-proto misc/schemas/{{entity}}.json misc/proto/{{entity}}.proto

# Compile a proto file into a pb2 file
compile_proto entity:
 mkdir -p src/bigquery_storage_write_api_examples/entities/{{entity}}/
 protoc --proto_path=misc/proto/ {{entity}}.proto --python_out=src/bigquery_storage_write_api_examples/entities/{{entity}}/

bq_init:
  uv run examples bq-init

default_stream_example:
  LINE_PROFILE=1 uv run examples run default-stream-writer

pending_type_stream_example:
  LINE_PROFILE=1 uv run examples run pending-type-stream-writer
