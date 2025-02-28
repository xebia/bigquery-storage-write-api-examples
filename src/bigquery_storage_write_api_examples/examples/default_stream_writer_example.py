from google.cloud.bigquery_storage_v1 import BigQueryWriteClient
from google.cloud.bigquery_storage_v1.types import (
    AppendRowsRequest,
    AppendRowsResponse,
    ProtoRows,
    ProtoSchema,
)
from google.cloud.bigquery_storage_v1.writer import AppendRowsStream
from google.protobuf.descriptor_pb2 import DescriptorProto
from google.protobuf.json_format import ParseDict
from line_profiler import profile

from bigquery_storage_write_api_examples import Config
from bigquery_storage_write_api_examples.data_generator import FakeDataGenerator
from bigquery_storage_write_api_examples.entities.students.students_pb2 import (
    RawStudents,
)
from bigquery_storage_write_api_examples.examples import BigQueryWriterExample


class DefaultStreamWriterExample(BigQueryWriterExample):
    def __init__(self, config: Config):
        super().__init__(project_id=config.gcp_project_id)
        self.dataset_id = config.gcp_dataset_id
        self.table_id = "students"
        self._init_stream()

    def _init_stream(self):
        self.write_client = BigQueryWriteClient()
        self.table_path = self.write_client.table_path(self.project_id, self.dataset_id, self.table_id)
        self.stream_name = self.write_client.write_stream_path(
            self.project_id, self.dataset_id, self.table_id, "_default"
        )
        self.proto_descriptor: DescriptorProto = DescriptorProto()
        RawStudents.DESCRIPTOR.CopyToProto(self.proto_descriptor)
        self.proto_schema = ProtoSchema(proto_descriptor=self.proto_descriptor)
        self.proto_data: AppendRowsRequest.ProtoData = AppendRowsRequest.ProtoData()
        self.proto_data.writer_schema = self.proto_schema

        self.request_template = AppendRowsRequest()
        self.request_template.write_stream = self.stream_name
        self.request_template.proto_rows = self.proto_data

        self.append_rows_stream: AppendRowsStream = AppendRowsStream(self.write_client, self.request_template)

    def _request(self, students: list[dict]) -> AppendRowsRequest:
        request = AppendRowsRequest()
        proto_data = AppendRowsRequest.ProtoData()

        proto_rows = ProtoRows()

        for student in students:
            raw_student = ParseDict(js_dict=student, message=RawStudents(), ignore_unknown_fields=True)
            proto_rows.serialized_rows.append(raw_student.SerializeToString())

        proto_data.rows = proto_rows
        request.proto_rows = proto_data
        return request

    def run(self):
        self.logger.info("✨ Generating fake students data")
        number_of_students = 1_000
        fake_students = FakeDataGenerator().generate_fake_students_data(number_of_students)

        self.logger.debug(f"🎓 Generated {len(fake_students)} fake students")

        self.logger.debug("📦 Generating request")
        request = self._request(fake_students)

        self.logger.debug("🚀 Sending request")
        response = self._write_students(request)

        self.logger.debug(f"🎓 Result: {response}")

        self.logger.debug("✅ Data is written to BigQuery table")

    @profile
    def _write_students(self, request: AppendRowsRequest) -> AppendRowsResponse:
        self.logger.debug("Sending a request to BigQuery")
        response_future = self.append_rows_stream.send(request)
        # if this doesn't raise an exception, all rows are considered successful
        result = response_future.result()
        self.logger.debug(f"🎓 Result: {result}")
        return result
