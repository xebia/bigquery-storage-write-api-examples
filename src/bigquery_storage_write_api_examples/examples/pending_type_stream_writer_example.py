import logging

from google.cloud import bigquery_storage_v1
from google.cloud.bigquery_storage_v1 import types, writer
from google.protobuf import descriptor_pb2
from google.protobuf.json_format import ParseDict
from line_profiler import profile

from bigquery_storage_write_api_examples import Config
from bigquery_storage_write_api_examples.entities.courses.courses_pb2 import RawCourses
from bigquery_storage_write_api_examples.fake_data_generator import FakeDataGenerator


class PendingTypeStreamWriterExample:
    """
    This example demonstrates how to insert data into a BigQuery table using the Pending Type Stream Writer.

    Use this writer when you want to add data to a table and commit it at a later time; until you commit,
    the data will not appear in query results.

    For more information, see:
        - https://cloud.google.com/bigquery/docs/write-api-batch
    """

    def __init__(self, config: Config):
        self.logger = logging.getLogger(__name__)
        self.project_id = config.gcp_project_id
        self.dataset_id = config.gcp_dataset_id
        self.table_id = "courses"

        self._init_stream()

    def _init_stream(self):
        # """Create a write stream, write some data, and commit the stream."""
        self.write_client = bigquery_storage_v1.BigQueryWriteClient()
        self.table_path = self.write_client.table_path(self.project_id, self.dataset_id, self.table_id)

        self.write_stream = types.WriteStream()

        # When creating the stream, choose the type. Use the PENDING type to wait
        # until the stream is committed before it is visible. See:
        # https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1#google.cloud.bigquery.storage.v1.WriteStream.Type
        self.write_stream.type_ = types.WriteStream.Type.PENDING
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
        RawCourses.DESCRIPTOR.CopyToProto(self.proto_descriptor)
        self.proto_schema.proto_descriptor = self.proto_descriptor
        self.proto_data = types.AppendRowsRequest.ProtoData()
        self.proto_data.writer_schema = self.proto_schema
        self.request_template.proto_rows = self.proto_data

        # Some stream types support an unbounded number of requests. Construct an
        # AppendRowsStream to send an arbitrary number of requests to a stream.
        self.append_rows_stream = writer.AppendRowsStream(self.write_client, self.request_template)

    def _request(self, courses: list[dict], offset: int) -> types.AppendRowsRequest:
        request = types.AppendRowsRequest()
        request.offset = offset
        proto_data = types.AppendRowsRequest.ProtoData()

        proto_rows = types.ProtoRows()

        for course in courses:
            raw_course = ParseDict(js_dict=course, message=RawCourses(), ignore_unknown_fields=True)
            proto_rows.serialized_rows.append(raw_course.SerializeToString())

        proto_data.rows = proto_rows
        request.proto_rows = proto_data
        return request

    def run(self):
        self.logger.info("📚 Generating fake courses data")
        number_of_batches = 2
        number_of_courses = 10

        faker = FakeDataGenerator()

        batches = [faker.generate_fake_courses(number_of_courses) for _ in range(number_of_batches)]

        self.logger.debug(f"📦 Generated {len(batches)} batches with {number_of_courses} courses each")

        # Set an offset to allow resuming this stream if the connection breaks.
        # Keep track of which requests the server has acknowledged and resume the
        # stream at the first non-acknowledged message. If the server has already
        # processed a message with that offset, it will return an ALREADY_EXISTS
        # error, which can be safely ignored.
        #
        # The first request must always have an offset of 0.
        offset = 0

        for batch_index, batch in enumerate(batches):
            request = self._request(batch, offset)
            self._write_courses(request=request, batch_index=batch_index, batch_size=len(batch))
            # Offset must equal the number of rows that were previously sent.
            offset += len(batch)
            input("Press Enter to continue...")

        # Send another batch.
        # Shutdown background threads and close the streaming connection.
        self.logger.info("⏹️ Closing append rows stream")
        self.append_rows_stream.close()

        # A PENDING type stream must be "finalized" before being committed. No new
        # records can be written to the stream after this method has been called.
        self.logger.info("🏁 Finalizing write stream")
        self.write_client.finalize_write_stream(name=self.write_stream.name)

        # Commit the stream you created earlier.
        self.logger.info("🚀 Committing write stream")
        batch_commit_write_streams_request = types.BatchCommitWriteStreamsRequest()
        batch_commit_write_streams_request.parent = self.table_path
        batch_commit_write_streams_request.write_streams = [self.write_stream.name]
        self.write_client.batch_commit_write_streams(batch_commit_write_streams_request)

        self.logger.info(f"✅ Writes to stream: '{self.write_stream.name}' have been committed")

    @profile
    def _write_courses(
        self, request: types.AppendRowsRequest, batch_index: int, batch_size: int
    ) -> types.AppendRowsResponse:
        response_future = self.append_rows_stream.send(request)
        self.logger.info(f"🎓 Sent batch {batch_index} with {batch_size} courses")
        result = response_future.result()
        self.logger.info(f"🎓 Result for batch {batch_index} is {result}")

        return result
