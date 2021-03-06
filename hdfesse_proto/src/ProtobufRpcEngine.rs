// This file is generated by rust-protobuf 2.23.0. Do not edit
// @generated

// https://github.com/rust-lang/rust-clippy/issues/702
#![allow(unknown_lints)]
#![allow(clippy::all)]

#![allow(unused_attributes)]
#![cfg_attr(rustfmt, rustfmt::skip)]

#![allow(box_pointers)]
#![allow(dead_code)]
#![allow(missing_docs)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(trivial_casts)]
#![allow(unused_imports)]
#![allow(unused_results)]
//! Generated file from `ProtobufRpcEngine.proto`

/// Generated files are compatible only with the same version
/// of protobuf runtime.
// const _PROTOBUF_VERSION_CHECK: () = ::protobuf::VERSION_2_23_0;

#[derive(PartialEq,Clone,Default)]
pub struct RequestHeaderProto {
    // message fields
    methodName: ::protobuf::SingularField<::std::string::String>,
    declaringClassProtocolName: ::protobuf::SingularField<::std::string::String>,
    clientProtocolVersion: ::std::option::Option<u64>,
    // special fields
    pub unknown_fields: ::protobuf::UnknownFields,
    pub cached_size: ::protobuf::CachedSize,
}

impl<'a> ::std::default::Default for &'a RequestHeaderProto {
    fn default() -> &'a RequestHeaderProto {
        <RequestHeaderProto as ::protobuf::Message>::default_instance()
    }
}

impl RequestHeaderProto {
    pub fn new() -> RequestHeaderProto {
        ::std::default::Default::default()
    }

    // required string methodName = 1;


    pub fn get_methodName(&self) -> &str {
        match self.methodName.as_ref() {
            Some(v) => &v,
            None => "",
        }
    }
    pub fn clear_methodName(&mut self) {
        self.methodName.clear();
    }

    pub fn has_methodName(&self) -> bool {
        self.methodName.is_some()
    }

    // Param is passed by value, moved
    pub fn set_methodName(&mut self, v: ::std::string::String) {
        self.methodName = ::protobuf::SingularField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_methodName(&mut self) -> &mut ::std::string::String {
        if self.methodName.is_none() {
            self.methodName.set_default();
        }
        self.methodName.as_mut().unwrap()
    }

    // Take field
    pub fn take_methodName(&mut self) -> ::std::string::String {
        self.methodName.take().unwrap_or_else(|| ::std::string::String::new())
    }

    // required string declaringClassProtocolName = 2;


    pub fn get_declaringClassProtocolName(&self) -> &str {
        match self.declaringClassProtocolName.as_ref() {
            Some(v) => &v,
            None => "",
        }
    }
    pub fn clear_declaringClassProtocolName(&mut self) {
        self.declaringClassProtocolName.clear();
    }

    pub fn has_declaringClassProtocolName(&self) -> bool {
        self.declaringClassProtocolName.is_some()
    }

    // Param is passed by value, moved
    pub fn set_declaringClassProtocolName(&mut self, v: ::std::string::String) {
        self.declaringClassProtocolName = ::protobuf::SingularField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_declaringClassProtocolName(&mut self) -> &mut ::std::string::String {
        if self.declaringClassProtocolName.is_none() {
            self.declaringClassProtocolName.set_default();
        }
        self.declaringClassProtocolName.as_mut().unwrap()
    }

    // Take field
    pub fn take_declaringClassProtocolName(&mut self) -> ::std::string::String {
        self.declaringClassProtocolName.take().unwrap_or_else(|| ::std::string::String::new())
    }

    // required uint64 clientProtocolVersion = 3;


    pub fn get_clientProtocolVersion(&self) -> u64 {
        self.clientProtocolVersion.unwrap_or(0)
    }
    pub fn clear_clientProtocolVersion(&mut self) {
        self.clientProtocolVersion = ::std::option::Option::None;
    }

    pub fn has_clientProtocolVersion(&self) -> bool {
        self.clientProtocolVersion.is_some()
    }

    // Param is passed by value, moved
    pub fn set_clientProtocolVersion(&mut self, v: u64) {
        self.clientProtocolVersion = ::std::option::Option::Some(v);
    }
}

impl ::protobuf::Message for RequestHeaderProto {
    fn is_initialized(&self) -> bool {
        if self.methodName.is_none() {
            return false;
        }
        if self.declaringClassProtocolName.is_none() {
            return false;
        }
        if self.clientProtocolVersion.is_none() {
            return false;
        }
        true
    }

    fn merge_from(&mut self, is: &mut ::protobuf::CodedInputStream<'_>) -> ::protobuf::ProtobufResult<()> {
        while !is.eof()? {
            let (field_number, wire_type) = is.read_tag_unpack()?;
            match field_number {
                1 => {
                    ::protobuf::rt::read_singular_string_into(wire_type, is, &mut self.methodName)?;
                },
                2 => {
                    ::protobuf::rt::read_singular_string_into(wire_type, is, &mut self.declaringClassProtocolName)?;
                },
                3 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    let tmp = is.read_uint64()?;
                    self.clientProtocolVersion = ::std::option::Option::Some(tmp);
                },
                _ => {
                    ::protobuf::rt::read_unknown_or_skip_group(field_number, wire_type, is, self.mut_unknown_fields())?;
                },
            };
        }
        ::std::result::Result::Ok(())
    }

    // Compute sizes of nested messages
    #[allow(unused_variables)]
    fn compute_size(&self) -> u32 {
        let mut my_size = 0;
        if let Some(ref v) = self.methodName.as_ref() {
            my_size += ::protobuf::rt::string_size(1, &v);
        }
        if let Some(ref v) = self.declaringClassProtocolName.as_ref() {
            my_size += ::protobuf::rt::string_size(2, &v);
        }
        if let Some(v) = self.clientProtocolVersion {
            my_size += ::protobuf::rt::value_size(3, v, ::protobuf::wire_format::WireTypeVarint);
        }
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream<'_>) -> ::protobuf::ProtobufResult<()> {
        if let Some(ref v) = self.methodName.as_ref() {
            os.write_string(1, &v)?;
        }
        if let Some(ref v) = self.declaringClassProtocolName.as_ref() {
            os.write_string(2, &v)?;
        }
        if let Some(v) = self.clientProtocolVersion {
            os.write_uint64(3, v)?;
        }
        os.write_unknown_fields(self.get_unknown_fields())?;
        ::std::result::Result::Ok(())
    }

    fn get_cached_size(&self) -> u32 {
        self.cached_size.get()
    }

    fn get_unknown_fields(&self) -> &::protobuf::UnknownFields {
        &self.unknown_fields
    }

    fn mut_unknown_fields(&mut self) -> &mut ::protobuf::UnknownFields {
        &mut self.unknown_fields
    }

    fn as_any(&self) -> &dyn (::std::any::Any) {
        self as &dyn (::std::any::Any)
    }
    fn as_any_mut(&mut self) -> &mut dyn (::std::any::Any) {
        self as &mut dyn (::std::any::Any)
    }
    fn into_any(self: ::std::boxed::Box<Self>) -> ::std::boxed::Box<dyn (::std::any::Any)> {
        self
    }

    fn descriptor(&self) -> &'static ::protobuf::reflect::MessageDescriptor {
        Self::descriptor_static()
    }

    fn new() -> RequestHeaderProto {
        RequestHeaderProto::new()
    }

    fn descriptor_static() -> &'static ::protobuf::reflect::MessageDescriptor {
        static descriptor: ::protobuf::rt::LazyV2<::protobuf::reflect::MessageDescriptor> = ::protobuf::rt::LazyV2::INIT;
        descriptor.get(|| {
            let mut fields = ::std::vec::Vec::new();
            fields.push(::protobuf::reflect::accessor::make_singular_field_accessor::<_, ::protobuf::types::ProtobufTypeString>(
                "methodName",
                |m: &RequestHeaderProto| { &m.methodName },
                |m: &mut RequestHeaderProto| { &mut m.methodName },
            ));
            fields.push(::protobuf::reflect::accessor::make_singular_field_accessor::<_, ::protobuf::types::ProtobufTypeString>(
                "declaringClassProtocolName",
                |m: &RequestHeaderProto| { &m.declaringClassProtocolName },
                |m: &mut RequestHeaderProto| { &mut m.declaringClassProtocolName },
            ));
            fields.push(::protobuf::reflect::accessor::make_option_accessor::<_, ::protobuf::types::ProtobufTypeUint64>(
                "clientProtocolVersion",
                |m: &RequestHeaderProto| { &m.clientProtocolVersion },
                |m: &mut RequestHeaderProto| { &mut m.clientProtocolVersion },
            ));
            ::protobuf::reflect::MessageDescriptor::new_pb_name::<RequestHeaderProto>(
                "RequestHeaderProto",
                fields,
                file_descriptor_proto()
            )
        })
    }

    fn default_instance() -> &'static RequestHeaderProto {
        static instance: ::protobuf::rt::LazyV2<RequestHeaderProto> = ::protobuf::rt::LazyV2::INIT;
        instance.get(RequestHeaderProto::new)
    }
}

impl ::protobuf::Clear for RequestHeaderProto {
    fn clear(&mut self) {
        self.methodName.clear();
        self.declaringClassProtocolName.clear();
        self.clientProtocolVersion = ::std::option::Option::None;
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for RequestHeaderProto {
    fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for RequestHeaderProto {
    fn as_ref(&self) -> ::protobuf::reflect::ReflectValueRef {
        ::protobuf::reflect::ReflectValueRef::Message(self)
    }
}

static file_descriptor_proto_data: &'static [u8] = b"\
    \n\x17ProtobufRpcEngine.proto\x12\rhadoop.common\"\xaa\x01\n\x12RequestH\
    eaderProto\x12\x1e\n\nmethodName\x18\x01\x20\x02(\tR\nmethodName\x12>\n\
    \x1adeclaringClassProtocolName\x18\x02\x20\x02(\tR\x1adeclaringClassProt\
    ocolName\x124\n\x15clientProtocolVersion\x18\x03\x20\x02(\x04R\x15client\
    ProtocolVersionB<\n\x1eorg.apache.hadoop.ipc.protobufB\x17ProtobufRpcEng\
    ineProtos\xa0\x01\x01\
";

static file_descriptor_proto_lazy: ::protobuf::rt::LazyV2<::protobuf::descriptor::FileDescriptorProto> = ::protobuf::rt::LazyV2::INIT;

fn parse_descriptor_proto() -> ::protobuf::descriptor::FileDescriptorProto {
    ::protobuf::Message::parse_from_bytes(file_descriptor_proto_data).unwrap()
}

pub fn file_descriptor_proto() -> &'static ::protobuf::descriptor::FileDescriptorProto {
    file_descriptor_proto_lazy.get(|| {
        parse_descriptor_proto()
    })
}
