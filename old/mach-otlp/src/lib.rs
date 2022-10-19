#![allow(warnings)]
#![feature(vec_into_raw_parts)]
use std::convert::From;

pub mod opentelemetry {
    pub mod proto {
        pub mod collector {
            pub mod logs {
                pub mod v1 {
                    tonic::include_proto!("opentelemetry.proto.collector.logs.v1");
                }
            }
            pub mod metrics {
                pub mod v1 {
                    tonic::include_proto!("opentelemetry.proto.collector.metrics.v1");
                }
            }
            pub mod trace {
                pub mod v1 {
                    tonic::include_proto!("opentelemetry.proto.collector.trace.v1");
                }
            }
        }
        pub mod logs {
            pub mod v1 {
                tonic::include_proto!("opentelemetry.proto.logs.v1");
            }
        }
        pub mod metrics {
            pub mod v1 {
                tonic::include_proto!("opentelemetry.proto.metrics.v1");
            }
        }
        pub mod trace {
            pub mod v1 {
                tonic::include_proto!("opentelemetry.proto.trace.v1");
            }
        }
        use otlp::common;
        use otlp::resource;
    }
}
use mach::sample::SampleType;
pub use opentelemetry::proto::*;

impl From<&otlp::logs::v1::SeverityNumber> for logs::v1::SeverityNumber {
    fn from(item: &otlp::logs::v1::SeverityNumber) -> Self {
        item.into()
    }
}

/* Logs conversion */
impl From<&otlp::logs::v1::ResourceLogs> for logs::v1::ResourceLogs {
    fn from(item: &otlp::logs::v1::ResourceLogs) -> Self {
        Self {
            resource: item.resource.clone(),
            scope_logs: item.scope_logs.iter().map(|x| x.into()).collect(),
            instrumentation_library_logs: Vec::new(),
            schema_url: item.schema_url.clone(),
        }
    }
}

impl From<&otlp::logs::v1::ScopeLogs> for logs::v1::ScopeLogs {
    fn from(item: &otlp::logs::v1::ScopeLogs) -> Self {
        Self {
            scope: item.scope.clone(),
            log_records: item.log_records.iter().map(|x| x.into()).collect(),
            schema_url: item.schema_url.clone(),
        }
    }
}

impl From<&otlp::logs::v1::LogRecord> for logs::v1::LogRecord {
    fn from(item: &otlp::logs::v1::LogRecord) -> Self {
        Self {
            time_unix_nano: item.time_unix_nano,
            severity_number: item.severity_number.into(),
            observed_time_unix_nano: item.observed_time_unix_nano.into(),
            severity_text: item.severity_text.clone(),
            body: item.body.clone(),
            attributes: item.attributes.clone(),
            dropped_attributes_count: item.dropped_attributes_count,
            flags: item.flags,
            trace_id: item.trace_id.clone(),
            span_id: item.span_id.clone(),
            source_id: u64::MAX,
        }
    }
}

impl logs::v1::ResourceLogs {
    pub fn set_source_id(&mut self) {
        let resource_attribs = &self.resource.as_ref().unwrap().attributes;
        let mut hash = 0u64;
        for attrib in resource_attribs.iter() {
            hash ^= fxhash::hash64(attrib);
        }
        for scope in self.scope_logs.iter_mut() {
            let scope_name = &scope.scope.as_ref().unwrap().name;
            let mut hash = hash;
            hash ^= fxhash::hash64(&scope_name);
            for log in scope.log_records.iter_mut() {
                //let mut hash = hash;
                //hash ^= fxhash::hash64(&span.name);
                log.source_id = hash;
            }
        }
    }
}

/* Spans conversion */
impl From<&otlp::trace::v1::status::StatusCode> for trace::v1::status::StatusCode {
    fn from(item: &otlp::trace::v1::status::StatusCode) -> Self {
        item.into()
    }
}

impl From<&otlp::trace::v1::span::SpanKind> for trace::v1::span::SpanKind {
    fn from(item: &otlp::trace::v1::span::SpanKind) -> Self {
        item.into()
    }
}

impl From<&otlp::trace::v1::span::Event> for trace::v1::span::Event {
    fn from(item: &otlp::trace::v1::span::Event) -> Self {
        Self {
            time_unix_nano: item.time_unix_nano,
            name: item.name.clone(),
            attributes: item.attributes.clone(),
            dropped_attributes_count: item.dropped_attributes_count,
        }
    }
}

impl From<&otlp::trace::v1::span::Link> for trace::v1::span::Link {
    fn from(item: &otlp::trace::v1::span::Link) -> Self {
        Self {
            trace_id: item.trace_id.clone(),
            span_id: item.span_id.clone(),
            trace_state: item.trace_state.clone(),
            attributes: item.attributes.clone(),
            dropped_attributes_count: item.dropped_attributes_count.clone(),
        }
    }
}

impl From<&otlp::trace::v1::Status> for trace::v1::Status {
    fn from(item: &otlp::trace::v1::Status) -> Self {
        Self {
            message: item.message.clone(),
            code: item.code.into(),
        }
    }
}

impl From<&otlp::trace::v1::Span> for trace::v1::Span {
    fn from(item: &otlp::trace::v1::Span) -> Self {
        Self {
            trace_id: item.trace_id.clone(),
            span_id: item.span_id.clone(),
            trace_state: item.trace_state.clone(),
            parent_span_id: item.parent_span_id.clone(),
            name: item.name.clone(),
            kind: item.kind.into(),
            start_time_unix_nano: item.start_time_unix_nano,
            end_time_unix_nano: item.end_time_unix_nano,
            attributes: item.attributes.clone(),
            dropped_attributes_count: item.dropped_attributes_count,
            events: item.events.iter().map(|x| x.into()).collect(),
            dropped_events_count: item.dropped_events_count,
            links: item.links.iter().map(|x| x.into()).collect(),
            dropped_links_count: item.dropped_links_count,
            status: match &item.status {
                None => None,
                Some(x) => Some(x.into()),
            },
            source_id: u64::MAX,
        }
    }
}

impl From<&SampleType> for trace::v1::Span {
    fn from(item: &SampleType) -> Self {
        match item {
            SampleType::Bytes(x) => bincode::deserialize(x).unwrap(),
            _ => panic!("unsupported sample type"),
        }
    }
}

impl From<&otlp::trace::v1::ScopeSpans> for trace::v1::ScopeSpans {
    fn from(item: &otlp::trace::v1::ScopeSpans) -> Self {
        Self {
            scope: item.scope.clone(),
            spans: item.spans.iter().map(|x| x.into()).collect(),
            schema_url: item.schema_url.clone(),
        }
    }
}

impl From<&otlp::trace::v1::ResourceSpans> for trace::v1::ResourceSpans {
    fn from(item: &otlp::trace::v1::ResourceSpans) -> Self {
        Self {
            resource: item.resource.clone(),
            scope_spans: item.scope_spans.iter().map(|x| x.into()).collect(),
            instrumentation_library_spans: Vec::new(),
            schema_url: item.schema_url.clone(),
        }
    }
}

impl trace::v1::ResourceSpans {
    pub fn set_source_id(&mut self) {
        let resource_attribs = &self.resource.as_ref().unwrap().attributes;
        let mut hash = 0u64;
        for attrib in resource_attribs.iter() {
            hash ^= fxhash::hash64(attrib);
        }
        for scope in self.scope_spans.iter_mut() {
            let scope_name = &scope.scope.as_ref().unwrap().name;
            let mut hash = hash;
            hash ^= fxhash::hash64(&scope_name);
            for span in scope.spans.iter_mut() {
                let mut hash = hash;
                hash ^= fxhash::hash64(&span.name);
                span.source_id = hash;
            }
        }
    }

    //pub fn write_samples_to_mach<F: FnMut(mach::id::SeriesId, u64, &[mach::sample::Type])>(
    //    &self,
    //    func: &mut F
    //){
    //    let mut v: Vec<mach::sample::Type> = Vec::with_capacity(1);
    //    v.push(mach::sample::Type::Bytes(Vec::with_capacity(8192)));
    //    for scope in &self.scope_spans {
    //        for span in &scope.spans {
    //            let buf = v[0].byte_vec_mut();
    //            let sz = bincode::serialized_size(&span).unwrap() as usize;
    //            if buf.len() < sz {
    //                buf.resize(sz, 0);
    //            }
    //            bincode::serialize_into(&mut buf[..], &span).unwrap();
    //            let series_id = mach::id::SeriesId(span.source_id);
    //            func(series_id, span.start_time_unix_nano, v.as_slice());
    //        }
    //    }
    //}

    pub fn get_samples(&self, samples: &mut Vec<(mach::id::SeriesId, u64, Vec<SampleType>)>) {
        for scope in &self.scope_spans {
            for span in &scope.spans {
                let mut v = Vec::with_capacity(1);
                v.push(SampleType::Bytes(bincode::serialize(&span).unwrap()));
                let series_id = mach::id::SeriesId(span.source_id);
                samples.push((series_id, span.start_time_unix_nano, v));
            }
        }
    }
}

// Wrapper
#[derive(serde::Serialize, serde::Deserialize, Clone)]
pub enum OtlpData {
    Logs(Vec<logs::v1::ResourceLogs>),
    Metrics(Vec<metrics::v1::ResourceMetrics>),
    Spans(Vec<trace::v1::ResourceSpans>),
}

impl From<&otlp::OtlpData> for OtlpData {
    fn from(item: &otlp::OtlpData) -> Self {
        match item {
            otlp::OtlpData::Logs(x) => Self::Logs(x.iter().map(|x| x.into()).collect()),
            otlp::OtlpData::Metrics(x) => unimplemented!(),
            otlp::OtlpData::Spans(x) => Self::Spans(x.iter().map(|x| x.into()).collect()),
        }
    }
}
