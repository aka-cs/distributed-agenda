# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: proto/events.proto
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import message as _message
from google.protobuf import reflection as _reflection
from google.protobuf import symbol_database as _symbol_database
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()


from google.protobuf import timestamp_pb2 as google_dot_protobuf_dot_timestamp__pb2


DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\x12proto/events.proto\x12\x12\x64istributed_agenda\x1a\x1fgoogle/protobuf/timestamp.proto\"\xaa\x01\n\x05\x45vent\x12\n\n\x02id\x18\x01 \x01(\x03\x12)\n\x05start\x18\x02 \x01(\x0b\x32\x1a.google.protobuf.Timestamp\x12\'\n\x03\x65nd\x18\x03 \x01(\x0b\x32\x1a.google.protobuf.Timestamp\x12\x0c\n\x04name\x18\x04 \x01(\t\x12\x13\n\x0b\x64\x65scription\x18\x05 \x01(\t\x12\x0f\n\x07groupId\x18\x06 \x01(\x03\x12\r\n\x05\x64raft\x18\x07 \x01(\x08\"\x92\x01\n\rConfirmations\x12K\n\rConfirmations\x18\x01 \x03(\x0b\x32\x34.distributed_agenda.Confirmations.ConfirmationsEntry\x1a\x34\n\x12\x43onfirmationsEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\r\n\x05value\x18\x02 \x01(\x08:\x02\x38\x01\"\x1d\n\x0fGetEventRequest\x12\n\n\x02id\x18\x01 \x01(\x03\">\n\x12\x43reateEventRequest\x12(\n\x05\x65vent\x18\x01 \x01(\x0b\x32\x19.distributed_agenda.Event\" \n\x12\x44\x65leteEventRequest\x12\n\n\x02id\x18\x01 \x01(\x03\"<\n\x10GetEventResponse\x12(\n\x05\x65vent\x18\x01 \x01(\x0b\x32\x19.distributed_agenda.Event\"*\n\x13\x43reateEventResponse\x12\x13\n\x0bunavailable\x18\x01 \x03(\t\"\x15\n\x13\x44\x65leteEventResponse\"&\n\x13\x43onfirmEventRequest\x12\x0f\n\x07\x65ventId\x18\x01 \x01(\x03\"\x16\n\x14\x43onfirmEventResponse\"%\n\x12RejectEventRequest\x12\x0f\n\x07\x65ventId\x18\x01 \x01(\x03\"\x15\n\x13RejectEventResponse2\xf3\x03\n\rEventsService\x12W\n\x08GetEvent\x12#.distributed_agenda.GetEventRequest\x1a$.distributed_agenda.GetEventResponse\"\x00\x12`\n\x0b\x43reateEvent\x12&.distributed_agenda.CreateEventRequest\x1a\'.distributed_agenda.CreateEventResponse\"\x00\x12`\n\x0b\x44\x65leteEvent\x12&.distributed_agenda.DeleteEventRequest\x1a\'.distributed_agenda.DeleteEventResponse\"\x00\x12\x63\n\x0c\x43onfirmEvent\x12\'.distributed_agenda.ConfirmEventRequest\x1a(.distributed_agenda.ConfirmEventResponse\"\x00\x12`\n\x0bRejectEvent\x12&.distributed_agenda.RejectEventRequest\x1a\'.distributed_agenda.RejectEventResponse\"\x00\x42\x10Z\x0e./server/protob\x06proto3')



_EVENT = DESCRIPTOR.message_types_by_name['Event']
_CONFIRMATIONS = DESCRIPTOR.message_types_by_name['Confirmations']
_CONFIRMATIONS_CONFIRMATIONSENTRY = _CONFIRMATIONS.nested_types_by_name['ConfirmationsEntry']
_GETEVENTREQUEST = DESCRIPTOR.message_types_by_name['GetEventRequest']
_CREATEEVENTREQUEST = DESCRIPTOR.message_types_by_name['CreateEventRequest']
_DELETEEVENTREQUEST = DESCRIPTOR.message_types_by_name['DeleteEventRequest']
_GETEVENTRESPONSE = DESCRIPTOR.message_types_by_name['GetEventResponse']
_CREATEEVENTRESPONSE = DESCRIPTOR.message_types_by_name['CreateEventResponse']
_DELETEEVENTRESPONSE = DESCRIPTOR.message_types_by_name['DeleteEventResponse']
_CONFIRMEVENTREQUEST = DESCRIPTOR.message_types_by_name['ConfirmEventRequest']
_CONFIRMEVENTRESPONSE = DESCRIPTOR.message_types_by_name['ConfirmEventResponse']
_REJECTEVENTREQUEST = DESCRIPTOR.message_types_by_name['RejectEventRequest']
_REJECTEVENTRESPONSE = DESCRIPTOR.message_types_by_name['RejectEventResponse']
Event = _reflection.GeneratedProtocolMessageType('Event', (_message.Message,), {
  'DESCRIPTOR' : _EVENT,
  '__module__' : 'proto.events_pb2'
  # @@protoc_insertion_point(class_scope:distributed_agenda.Event)
  })
_sym_db.RegisterMessage(Event)

Confirmations = _reflection.GeneratedProtocolMessageType('Confirmations', (_message.Message,), {

  'ConfirmationsEntry' : _reflection.GeneratedProtocolMessageType('ConfirmationsEntry', (_message.Message,), {
    'DESCRIPTOR' : _CONFIRMATIONS_CONFIRMATIONSENTRY,
    '__module__' : 'proto.events_pb2'
    # @@protoc_insertion_point(class_scope:distributed_agenda.Confirmations.ConfirmationsEntry)
    })
  ,
  'DESCRIPTOR' : _CONFIRMATIONS,
  '__module__' : 'proto.events_pb2'
  # @@protoc_insertion_point(class_scope:distributed_agenda.Confirmations)
  })
_sym_db.RegisterMessage(Confirmations)
_sym_db.RegisterMessage(Confirmations.ConfirmationsEntry)

GetEventRequest = _reflection.GeneratedProtocolMessageType('GetEventRequest', (_message.Message,), {
  'DESCRIPTOR' : _GETEVENTREQUEST,
  '__module__' : 'proto.events_pb2'
  # @@protoc_insertion_point(class_scope:distributed_agenda.GetEventRequest)
  })
_sym_db.RegisterMessage(GetEventRequest)

CreateEventRequest = _reflection.GeneratedProtocolMessageType('CreateEventRequest', (_message.Message,), {
  'DESCRIPTOR' : _CREATEEVENTREQUEST,
  '__module__' : 'proto.events_pb2'
  # @@protoc_insertion_point(class_scope:distributed_agenda.CreateEventRequest)
  })
_sym_db.RegisterMessage(CreateEventRequest)

DeleteEventRequest = _reflection.GeneratedProtocolMessageType('DeleteEventRequest', (_message.Message,), {
  'DESCRIPTOR' : _DELETEEVENTREQUEST,
  '__module__' : 'proto.events_pb2'
  # @@protoc_insertion_point(class_scope:distributed_agenda.DeleteEventRequest)
  })
_sym_db.RegisterMessage(DeleteEventRequest)

GetEventResponse = _reflection.GeneratedProtocolMessageType('GetEventResponse', (_message.Message,), {
  'DESCRIPTOR' : _GETEVENTRESPONSE,
  '__module__' : 'proto.events_pb2'
  # @@protoc_insertion_point(class_scope:distributed_agenda.GetEventResponse)
  })
_sym_db.RegisterMessage(GetEventResponse)

CreateEventResponse = _reflection.GeneratedProtocolMessageType('CreateEventResponse', (_message.Message,), {
  'DESCRIPTOR' : _CREATEEVENTRESPONSE,
  '__module__' : 'proto.events_pb2'
  # @@protoc_insertion_point(class_scope:distributed_agenda.CreateEventResponse)
  })
_sym_db.RegisterMessage(CreateEventResponse)

DeleteEventResponse = _reflection.GeneratedProtocolMessageType('DeleteEventResponse', (_message.Message,), {
  'DESCRIPTOR' : _DELETEEVENTRESPONSE,
  '__module__' : 'proto.events_pb2'
  # @@protoc_insertion_point(class_scope:distributed_agenda.DeleteEventResponse)
  })
_sym_db.RegisterMessage(DeleteEventResponse)

ConfirmEventRequest = _reflection.GeneratedProtocolMessageType('ConfirmEventRequest', (_message.Message,), {
  'DESCRIPTOR' : _CONFIRMEVENTREQUEST,
  '__module__' : 'proto.events_pb2'
  # @@protoc_insertion_point(class_scope:distributed_agenda.ConfirmEventRequest)
  })
_sym_db.RegisterMessage(ConfirmEventRequest)

ConfirmEventResponse = _reflection.GeneratedProtocolMessageType('ConfirmEventResponse', (_message.Message,), {
  'DESCRIPTOR' : _CONFIRMEVENTRESPONSE,
  '__module__' : 'proto.events_pb2'
  # @@protoc_insertion_point(class_scope:distributed_agenda.ConfirmEventResponse)
  })
_sym_db.RegisterMessage(ConfirmEventResponse)

RejectEventRequest = _reflection.GeneratedProtocolMessageType('RejectEventRequest', (_message.Message,), {
  'DESCRIPTOR' : _REJECTEVENTREQUEST,
  '__module__' : 'proto.events_pb2'
  # @@protoc_insertion_point(class_scope:distributed_agenda.RejectEventRequest)
  })
_sym_db.RegisterMessage(RejectEventRequest)

RejectEventResponse = _reflection.GeneratedProtocolMessageType('RejectEventResponse', (_message.Message,), {
  'DESCRIPTOR' : _REJECTEVENTRESPONSE,
  '__module__' : 'proto.events_pb2'
  # @@protoc_insertion_point(class_scope:distributed_agenda.RejectEventResponse)
  })
_sym_db.RegisterMessage(RejectEventResponse)

_EVENTSSERVICE = DESCRIPTOR.services_by_name['EventsService']
if _descriptor._USE_C_DESCRIPTORS == False:

  DESCRIPTOR._options = None
  DESCRIPTOR._serialized_options = b'Z\016./server/proto'
  _CONFIRMATIONS_CONFIRMATIONSENTRY._options = None
  _CONFIRMATIONS_CONFIRMATIONSENTRY._serialized_options = b'8\001'
  _EVENT._serialized_start=76
  _EVENT._serialized_end=246
  _CONFIRMATIONS._serialized_start=249
  _CONFIRMATIONS._serialized_end=395
  _CONFIRMATIONS_CONFIRMATIONSENTRY._serialized_start=343
  _CONFIRMATIONS_CONFIRMATIONSENTRY._serialized_end=395
  _GETEVENTREQUEST._serialized_start=397
  _GETEVENTREQUEST._serialized_end=426
  _CREATEEVENTREQUEST._serialized_start=428
  _CREATEEVENTREQUEST._serialized_end=490
  _DELETEEVENTREQUEST._serialized_start=492
  _DELETEEVENTREQUEST._serialized_end=524
  _GETEVENTRESPONSE._serialized_start=526
  _GETEVENTRESPONSE._serialized_end=586
  _CREATEEVENTRESPONSE._serialized_start=588
  _CREATEEVENTRESPONSE._serialized_end=630
  _DELETEEVENTRESPONSE._serialized_start=632
  _DELETEEVENTRESPONSE._serialized_end=653
  _CONFIRMEVENTREQUEST._serialized_start=655
  _CONFIRMEVENTREQUEST._serialized_end=693
  _CONFIRMEVENTRESPONSE._serialized_start=695
  _CONFIRMEVENTRESPONSE._serialized_end=717
  _REJECTEVENTREQUEST._serialized_start=719
  _REJECTEVENTREQUEST._serialized_end=756
  _REJECTEVENTRESPONSE._serialized_start=758
  _REJECTEVENTRESPONSE._serialized_end=779
  _EVENTSSERVICE._serialized_start=782
  _EVENTSSERVICE._serialized_end=1281
# @@protoc_insertion_point(module_scope)
