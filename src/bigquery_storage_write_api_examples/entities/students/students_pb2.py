# mypy: ignore-errors
# isort: skip_file
# fmt: off

# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: students.proto
"""Generated protocol buffer code."""
from google.protobuf.internal import builder as _builder
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()




DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\x0estudents.proto\"\x8f\x03\n\x0bRawStudents\x12\x12\n\nstudent_id\x18\x01 \x01(\x03\x12\x12\n\nfirst_name\x18\x02 \x01(\t\x12\x11\n\tlast_name\x18\x03 \x01(\t\x12\x11\n\tbirthdate\x18\x04 \x01(\t\x12\x0c\n\x04year\x18\x05 \x01(\x03\x12/\n\x0c\x63ontact_info\x18\x06 \x01(\x0b\x32\x19.RawStudents.Contact_info\x12%\n\x07\x61\x64\x64ress\x18\x07 \x01(\x0b\x32\x14.RawStudents.Address\x12\x1a\n\x12\x65mergency_contacts\x18\x08 \x03(\t\x12\x12\n\nlast_login\x18\t \x01(\x03\x12\x10\n\x08metadata\x18\n \x01(\t\x1a,\n\x0c\x43ontact_info\x12\r\n\x05\x65mail\x18\x01 \x01(\t\x12\r\n\x05phone\x18\x02 \x01(\t\x1a\\\n\x07\x41\x64\x64ress\x12\x0e\n\x06street\x18\x01 \x01(\t\x12\x0c\n\x04\x63ity\x18\x02 \x01(\t\x12\r\n\x05state\x18\x03 \x01(\t\x12\x13\n\x0bpostal_code\x18\x04 \x01(\t\x12\x0f\n\x07\x63ountry\x18\x05 \x01(\tb\x06proto3')

_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'students_pb2', _globals)
if _descriptor._USE_C_DESCRIPTORS == False:

  DESCRIPTOR._options = None
  _globals['_RAWSTUDENTS']._serialized_start=19
  _globals['_RAWSTUDENTS']._serialized_end=418
  _globals['_RAWSTUDENTS_CONTACT_INFO']._serialized_start=280
  _globals['_RAWSTUDENTS_CONTACT_INFO']._serialized_end=324
  _globals['_RAWSTUDENTS_ADDRESS']._serialized_start=326
  _globals['_RAWSTUDENTS_ADDRESS']._serialized_end=418
# @@protoc_insertion_point(module_scope)
