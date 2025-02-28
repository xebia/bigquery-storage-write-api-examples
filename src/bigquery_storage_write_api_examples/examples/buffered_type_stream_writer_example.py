import logging

from google.api_core.exceptions import InvalidArgument
from google.cloud import bigquery_storage, bigquery_storage_v1
from google.cloud.bigquery_storage_v1 import types, writer
from google.protobuf import descriptor_pb2
from google.protobuf.json_format import ParseDict
from line_profiler import profile

from bigquery_storage_write_api_examples import Config
from bigquery_storage_write_api_examples.entities.classes.classes_pb2 import RawClasses
from bigquery_storage_write_api_examples.fake_data_generator import FakeDataGenerator


class BufferedTypeStreamWriterExample:
    """
    This example shows you how to add data to a BigQuery table using the Buffered Stream Writer.

    Choose this method when you need to add data in small batches and want to commit each batch separately.

    For more information, see:
        - https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1#google.cloud.bigquery.storage.v1.WriteStream.Type
    """

    def __init__(self, config: Config):
        self.logger = logging.getLogger(__name__)
        self.project_id = config.gcp_project_id
        self.dataset_id = config.gcp_dataset_id
        self.table_id = "classes"

        self._init_stream()

    def _init_stream(self):
        # """Create a write stream, write a batch of data and commit the stream for each batch"""
        self.write_client = bigquery_storage_v1.BigQueryWriteClient()
        self.table_path = self.write_client.table_path(self.project_id, self.dataset_id, self.table_id)

        self.write_stream = types.WriteStream()

        self.write_stream.type_ = types.WriteStream.Type.BUFFERED
        self.write_stream = self.write_client.create_write_stream(
            parent=self.table_path, write_stream=self.write_stream
        )
        self.stream_name = self.write_stream.name

        # Create a template with fields needed for the first request.
        self.request_template = types.AppendRowsRequest()

        # The initial request must contain the stream name.
        self.request_template.write_stream = self.stream_name

        # So that BigQuery knows how to parse the serialized_rows, generate a
        # protocol buffer representation of your message descriptor.
        self.proto_schema = types.ProtoSchema()
        self.proto_descriptor = descriptor_pb2.DescriptorProto()
        RawClasses.DESCRIPTOR.CopyToProto(self.proto_descriptor)
        self.proto_schema.proto_descriptor = self.proto_descriptor
        self.proto_data = types.AppendRowsRequest.ProtoData()
        self.proto_data.writer_schema = self.proto_schema
        self.request_template.proto_rows = self.proto_data

        # Some stream types support an unbounded number of requests. Construct an
        # AppendRowsStream to send an arbitrary number of requests to a stream.
        self.append_rows_stream = writer.AppendRowsStream(self.write_client, self.request_template)

    def _request(self, classes: list[dict], offset: int) -> types.AppendRowsRequest:
        request = types.AppendRowsRequest()
        request.offset = offset
        proto_data = types.AppendRowsRequest.ProtoData()

        proto_rows = types.ProtoRows()

        for class_ in classes:
            raw_class = ParseDict(js_dict=class_, message=RawClasses(), ignore_unknown_fields=True)
            proto_rows.serialized_rows.append(raw_class.SerializeToString())

        proto_data.rows = proto_rows
        request.proto_rows = proto_data
        return request

    def run(self):
        self.logger.info("📚 Generating fake classes data")
        number_of_batches = 3
        number_of_classes = 2

        faker = FakeDataGenerator()
        batches = [faker.generate_fake_classes(number_of_classes) for _ in range(number_of_batches)]
        self.logger.debug(f"📦 Generated {len(batches)} batches with {number_of_classes} classes each")

        # Set an offset to allow resuming this stream if the connection breaks.
        # Keep track of which requests the server has acknowledged and resume the
        # stream at the first non-acknowledged message. If the server has already
        # processed a message with that offset, it will return an ALREADY_EXISTS
        # error, which can be safely ignored.
        #
        # The first request must always have an offset of 0.
        offset = 0

        # For illustration purposes, we'll send one enrollment at a time.
        # In a real scenario, you can send a batch of enrollments at once if needed.
        for batch_index, batch in enumerate(batches):
            request = self._request(batch, offset)
            self._write_batch(request=request, batch_index=batch_index, batch_size=len(batch))

            request = bigquery_storage.FlushRowsRequest(write_stream=self.stream_name, offset=offset)
            self.append_rows_stream._client.flush_rows(request=request)

            # Offset must equal the number of rows that were previously sent.
            offset += len(batch)

            # The input() is used to pause the execution of the script to allow you to see the data in the table.
            input("Press Enter to continue...")

        # Send another batch.
        # Shutdown background threads and close the streaming connection.
        self.logger.info("⏹️ Closing append rows stream")
        self.append_rows_stream.close()

        self.logger.info("🏁 Finalizing write stream")
        self.write_client.finalize_write_stream(name=self.write_stream.name)

        # No need to commit the stream, it will be committed automatically
        self.logger.info(f"✅ Writes to stream: '{self.write_stream.name}' have been committed")

    @profile
    def _write_batch(
        self, request: types.AppendRowsRequest, batch_index: int, batch_size: int
    ) -> types.AppendRowsResponse:
        response_future = self.append_rows_stream.send(request)
        self.logger.info(f"🎓 Sending batch {batch_index} with {batch_size} classes")
        try:
            result = response_future.result()
            self.logger.info(f"🎓 Result for batch {batch_index} is {result}")
        except InvalidArgument as e:
            self.logger.error(f"🚨 Error for batch {batch_index}: {e.message}")
            self.logger.error(f"🚨 Response for batch {batch_index}: {e.response}")
            raise e

        return result
