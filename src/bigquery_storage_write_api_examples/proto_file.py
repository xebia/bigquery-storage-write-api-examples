class ProtoFileGenerator:

    @staticmethod
    def bigquery_field_to_proto_field(field, field_number, indent_level):
        indent = "    " * indent_level
        bq_type = field["type"]
        bq_mode = field.get("mode", None)  # 'NULLABLE', 'REQUIRED', 'REPEATED'
        field_name = field["name"]

        type_mapping = {
            "STRING": "string",
            "BYTES": "bytes",
            "INTEGER": "int64",
            "INT64": "int64",
            "FLOAT": "double",
            "FLOAT64": "double",
            "BOOLEAN": "bool",
            "BOOL": "bool",
            "TIMESTAMP": "int64",
            "DATE": "string",
            "TIME": "string",
            "DATETIME": "string",
            "GEOGRAPHY": "string",
            "NUMERIC": "string",
            "BIGNUMERIC": "double",
            "RECORD": "message",  # Will generate nested message
            "JSON": "string",
        }
        repeated = "repeated " if bq_mode == "REPEATED" else ""
        if bq_type == "RECORD":
            nested_message_name = field_name.capitalize()
            nested_message = ProtoFileGenerator.generate_proto_message(
                field["fields"], nested_message_name, indent_level
            )
            proto_field = (
                f"{nested_message}\n{indent}{repeated}{nested_message_name} {field_name} = {field_number};"
            )
        else:
            proto_type = type_mapping[bq_type]
            proto_field = f"{indent}{repeated}{proto_type} {field_name} = {field_number};"

        return proto_field

    @staticmethod
    def generate_proto_message(schema, message_name, indent_level=0):
        indent = "    " * indent_level
        proto_lines = []
        proto_lines.append(f"{indent}message {message_name} " + "{")
        field_number = 1
        for field in schema:
            proto_field = ProtoFileGenerator.bigquery_field_to_proto_field(
                field, field_number, indent_level + 1
            )
            proto_lines.append(proto_field)
            field_number += 1
        proto_lines.append(f"{indent}}}")
        return "\n".join(proto_lines)

    @staticmethod
    def proto_file(entity_name: str, schema: dict) -> str:
        message_name = entity_name.title().replace("_", "")
        main_message = ProtoFileGenerator.generate_proto_message(schema, f"Raw{message_name}")

        proto_lines = []
        proto_lines.append('syntax = "proto3";')
        proto_lines.append("")

        proto_lines.append(main_message)
        protobuf = "\n".join(proto_lines)
        protobuf += "\n"
        return protobuf
