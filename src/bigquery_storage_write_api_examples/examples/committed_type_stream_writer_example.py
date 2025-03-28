import logging

from google.api_core.exceptions import InvalidArgument
from google.cloud import bigquery_storage_v1
from google.cloud.bigquery_storage_v1 import types, writer
from google.protobuf import descriptor_pb2
from google.protobuf.json_format import ParseDict
from line_profiler import profile

from bigquery_storage_write_api_examples import Config
from bigquery_storage_write_api_examples.entities.enrollments.enrollments_pb2 import (
    RawEnrollments,
)
from bigquery_storage_write_api_examples.fake_data_generator import FakeDataGenerator


class CommittedTypeStreamWriterExample:
    """
    This example demonstrates how to insert data into a BigQuery table using the Committed Type Stream Writer.

    Use this writer when you want to see the written data immediately in the table, and you want to get Exactly-Once delivery semantics.

    For more information, see:
        - https://cloud.google.com/bigquery/docs/write-api-streaming#exactly-once
    """

    def __init__(self, config: Config):
        self.logger = logging.getLogger(__name__)
        self.project_id = config.gcp_project_id
        self.dataset_id = config.gcp_dataset_id
        self.table_id = "enrollments"

        self._init_stream()

    def _init_stream(self):
        # """Create a write stream, write data which will be available immediately in the table"""
        self.write_client = bigquery_storage_v1.BigQueryWriteClient()
        self.table_path = self.write_client.table_path(self.project_id, self.dataset_id, self.table_id)

        write_stream = types.WriteStream()

        # Data will commit automatically and appear as soon as the write is acknowledged.
        # https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1#google.cloud.bigquery.storage.v1.WriteStream.Type
        write_stream.type_ = types.WriteStream.Type.COMMITTED
        self.write_stream = self.write_client.create_write_stream(
            parent=self.table_path, write_stream=write_stream
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
        RawEnrollments.DESCRIPTOR.CopyToProto(self.proto_descriptor)
        self.proto_schema.proto_descriptor = self.proto_descriptor
        self.proto_data = types.AppendRowsRequest.ProtoData()
        self.proto_data.writer_schema = self.proto_schema
        self.request_template.proto_rows = self.proto_data

        # Some stream types support an unbounded number of requests. Construct an
        # AppendRowsStream to send an arbitrary number of requests to a stream.
        self.append_rows_stream = writer.AppendRowsStream(self.write_client, self.request_template)

    def _request(self, enrollment: dict, offset: int) -> types.AppendRowsRequest:
        request = types.AppendRowsRequest()
        request.offset = offset
        proto_data = types.AppendRowsRequest.ProtoData()

        proto_rows = types.ProtoRows()

        raw_enrollment = ParseDict(js_dict=enrollment, message=RawEnrollments(), ignore_unknown_fields=True)
        proto_rows.serialized_rows.append(raw_enrollment.SerializeToString())

        proto_data.rows = proto_rows
        request.proto_rows = proto_data
        return request

    def run(self):
        self.logger.info("📚 Generating fake enrollments data")
        number_of_enrollments = 5

        faker = FakeDataGenerator()
        enrollments = faker.generate_fake_enrollments(number_of_enrollments)
        self.logger.debug(f"📦 Generated {number_of_enrollments} enrollments")

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
        for enrollment in enrollments:
            request = self._request(enrollment, offset)
            self._write_enrollment(request=request, enrollment_id=enrollment["enrollment_id"])
            # Offset must equal the number of rows that were previously sent.
            offset += 1

            # The input() is used to pause the execution of the script to allow you to see the data in the table.
            input("Press Enter to continue...")

        # Send another batch.
        # Shutdown background threads and close the streaming connection.
        self.logger.info("⏹️ Closing append rows stream")
        self.append_rows_stream.close()

        # A COMMITTED type stream can be "finalized" (no new records can be written to the stream after this method has been called)
        # This method is optional, but it's recommended to call it to ensure that the stream is properly finalized.
        self.logger.info("🏁 Finalizing write stream")
        self.write_client.finalize_write_stream(name=self.write_stream.name)

        # No need to commit the stream, it will be committed automatically
        self.logger.info(f"✅ Writes to stream: '{self.write_stream.name}' have been committed")

        self.append_rows_stream.close()

    @profile
    def _write_enrollment(
        self, request: types.AppendRowsRequest, enrollment_id: int
    ) -> types.AppendRowsResponse:
        response_future = self.append_rows_stream.send(request)
        self.logger.info(f"🎓 Sending enrollment {enrollment_id}")
        try:
            result = response_future.result()
            self.logger.info(f"🎓 Result for enrollment {enrollment_id} is {result}")
        except InvalidArgument as e:
            self.logger.error(f"🚨 Error {enrollment_id}: {e.message}")
            self.logger.error(f"🚨 Response {enrollment_id}: {e.response}")
            raise e

        return result
