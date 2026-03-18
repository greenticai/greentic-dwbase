#[cfg(target_arch = "wasm32")]
use std::collections::BTreeMap;

#[cfg(target_arch = "wasm32")]
use greentic_interfaces_guest::component_v0_6::node;
#[cfg(target_arch = "wasm32")]
use greentic_types::cbor::canonical;
#[cfg(target_arch = "wasm32")]
use greentic_types::schemas::common::schema_ir::{AdditionalProperties, SchemaIr};
#[cfg(target_arch = "wasm32")]
use greentic_types::schemas::component::v0_6_0::{ComponentInfo, I18nText};

pub mod i18n;
pub mod i18n_bundle;
pub mod qa;

const COMPONENT_NAME: &str = "dwbase";
#[cfg(target_arch = "wasm32")]
const COMPONENT_ORG: &str = "ai.greentic";
#[cfg(target_arch = "wasm32")]
const COMPONENT_VERSION: &str = env!("CARGO_PKG_VERSION");
#[cfg(target_arch = "wasm32")]
const DEFAULT_OPERATION: &str = "dwbase.configure";

#[cfg(target_arch = "wasm32")]
#[used]
#[unsafe(link_section = ".greentic.wasi")]
static WASI_TARGET_MARKER: [u8; 13] = *b"wasm32-wasip2";

#[cfg(target_arch = "wasm32")]
struct Component;

#[cfg(target_arch = "wasm32")]
impl node::Guest for Component {
    fn describe() -> node::ComponentDescriptor {
        let input_schema_cbor = input_schema_cbor();
        let output_schema_cbor = output_schema_cbor();
        let mut ops = vec![
            node::Op {
                name: DEFAULT_OPERATION.to_string(),
                summary: Some(
                    "Validate and normalize DWBase capability-pack configuration.".to_string(),
                ),
                input: node::IoSchema {
                    schema: node::SchemaSource::InlineCbor(input_schema_cbor.clone()),
                    content_type: "application/cbor".to_string(),
                    schema_version: None,
                },
                output: node::IoSchema {
                    schema: node::SchemaSource::InlineCbor(output_schema_cbor.clone()),
                    content_type: "application/cbor".to_string(),
                    schema_version: None,
                },
                examples: Vec::new(),
            },
            node::Op {
                name: "dwbase.requirements".to_string(),
                summary: Some(
                    "Report DWBase capability and public ingress requirements.".to_string(),
                ),
                input: node::IoSchema {
                    schema: node::SchemaSource::InlineCbor(input_schema_cbor.clone()),
                    content_type: "application/cbor".to_string(),
                    schema_version: None,
                },
                output: node::IoSchema {
                    schema: node::SchemaSource::InlineCbor(output_schema_cbor.clone()),
                    content_type: "application/cbor".to_string(),
                    schema_version: None,
                },
                examples: Vec::new(),
            },
            node::Op {
                name: "dwbase.echo".to_string(),
                summary: Some("Echo a DWBase request envelope for smoke testing.".to_string()),
                input: node::IoSchema {
                    schema: node::SchemaSource::InlineCbor(input_schema_cbor.clone()),
                    content_type: "application/cbor".to_string(),
                    schema_version: None,
                },
                output: node::IoSchema {
                    schema: node::SchemaSource::InlineCbor(output_schema_cbor.clone()),
                    content_type: "application/cbor".to_string(),
                    schema_version: None,
                },
                examples: Vec::new(),
            },
        ];
        ops.extend(vec![
            node::Op {
                name: "qa-spec".to_string(),
                summary: Some("Return QA spec for requested mode.".to_string()),
                input: node::IoSchema {
                    schema: node::SchemaSource::InlineCbor(input_schema_cbor.clone()),
                    content_type: "application/cbor".to_string(),
                    schema_version: None,
                },
                output: node::IoSchema {
                    schema: node::SchemaSource::InlineCbor(output_schema_cbor.clone()),
                    content_type: "application/cbor".to_string(),
                    schema_version: None,
                },
                examples: Vec::new(),
            },
            node::Op {
                name: "apply-answers".to_string(),
                summary: Some("Apply QA answers and return a config patch.".to_string()),
                input: node::IoSchema {
                    schema: node::SchemaSource::InlineCbor(input_schema_cbor.clone()),
                    content_type: "application/cbor".to_string(),
                    schema_version: None,
                },
                output: node::IoSchema {
                    schema: node::SchemaSource::InlineCbor(output_schema_cbor.clone()),
                    content_type: "application/cbor".to_string(),
                    schema_version: None,
                },
                examples: Vec::new(),
            },
            node::Op {
                name: "i18n-keys".to_string(),
                summary: Some("Return i18n keys referenced by QA/setup.".to_string()),
                input: node::IoSchema {
                    schema: node::SchemaSource::InlineCbor(input_schema_cbor.clone()),
                    content_type: "application/cbor".to_string(),
                    schema_version: None,
                },
                output: node::IoSchema {
                    schema: node::SchemaSource::InlineCbor(output_schema_cbor),
                    content_type: "application/cbor".to_string(),
                    schema_version: None,
                },
                examples: Vec::new(),
            },
        ]);
        node::ComponentDescriptor {
            name: COMPONENT_NAME.to_string(),
            version: COMPONENT_VERSION.to_string(),
            summary: Some(
                "DWBase Greentic capability provider with ingress-aware QA and configuration."
                    .to_string(),
            ),
            capabilities: Vec::new(),
            ops,
            schemas: Vec::new(),
            setup: None,
        }
    }

    fn invoke(
        operation: String,
        envelope: node::InvocationEnvelope,
    ) -> Result<node::InvocationResult, node::NodeError> {
        let output = run_component_cbor(&operation, envelope.payload_cbor);
        Ok(node::InvocationResult {
            ok: true,
            output_cbor: output,
            output_metadata_cbor: None,
        })
    }
}

#[cfg(target_arch = "wasm32")]
greentic_interfaces_guest::export_component_v060!(Component);

pub fn handle_message(operation: &str, input: &str) -> String {
    format!("{COMPONENT_NAME}::{operation} => {}", input.trim())
}

#[cfg(target_arch = "wasm32")]
fn encode_cbor<T: serde::Serialize>(value: &T) -> Vec<u8> {
    canonical::to_canonical_cbor_allow_floats(value).expect("encode cbor")
}

#[cfg(target_arch = "wasm32")]
fn parse_payload(input: &[u8]) -> serde_json::Value {
    if let Ok(value) = canonical::from_cbor(input) {
        return value;
    }
    serde_json::from_slice(input).unwrap_or_else(|_| serde_json::json!({}))
}

#[cfg(target_arch = "wasm32")]
fn normalized_mode(payload: &serde_json::Value) -> qa::NormalizedMode {
    let mode = payload
        .get("mode")
        .and_then(|v| v.as_str())
        .or_else(|| payload.get("operation").and_then(|v| v.as_str()))
        .unwrap_or("setup");
    qa::normalize_mode(mode).unwrap_or(qa::NormalizedMode::Setup)
}

#[cfg(target_arch = "wasm32")]
fn input_schema() -> SchemaIr {
    SchemaIr::Object {
        properties: BTreeMap::from([(
            "input".to_string(),
            SchemaIr::String {
                min_len: Some(0),
                max_len: None,
                regex: None,
                format: None,
            },
        )]),
        required: vec!["input".to_string()],
        additional: AdditionalProperties::Forbid,
    }
}

#[cfg(target_arch = "wasm32")]
fn output_schema() -> SchemaIr {
    SchemaIr::Object {
        properties: BTreeMap::from([(
            "message".to_string(),
            SchemaIr::String {
                min_len: Some(0),
                max_len: None,
                regex: None,
                format: None,
            },
        )]),
        required: vec!["message".to_string()],
        additional: AdditionalProperties::Forbid,
    }
}

#[cfg(target_arch = "wasm32")]
#[allow(dead_code)]
fn config_schema() -> SchemaIr {
    SchemaIr::Object {
        properties: BTreeMap::new(),
        required: Vec::new(),
        additional: AdditionalProperties::Forbid,
    }
}

#[cfg(target_arch = "wasm32")]
#[allow(dead_code)]
fn component_info() -> ComponentInfo {
    ComponentInfo {
        id: format!("{COMPONENT_ORG}.{COMPONENT_NAME}"),
        version: COMPONENT_VERSION.to_string(),
        role: "tool".to_string(),
        display_name: Some(I18nText::new(
            "component.display_name",
            Some(COMPONENT_NAME.to_string()),
        )),
    }
}

#[cfg(target_arch = "wasm32")]
fn input_schema_cbor() -> Vec<u8> {
    encode_cbor(&input_schema())
}

#[cfg(target_arch = "wasm32")]
fn output_schema_cbor() -> Vec<u8> {
    encode_cbor(&output_schema())
}

#[cfg(target_arch = "wasm32")]
fn run_component_cbor(operation: &str, input: Vec<u8>) -> Vec<u8> {
    let value = parse_payload(&input);
    let output = match operation {
        "dwbase.configure" => qa::configure(&value),
        "dwbase.requirements" => qa::requirements_json(),
        "qa-spec" => {
            let mode = normalized_mode(&value);
            qa::qa_spec_json(mode)
        }
        "apply-answers" => {
            let mode = normalized_mode(&value);
            qa::apply_answers(mode, &value)
        }
        "i18n-keys" => serde_json::Value::Array(
            qa::i18n_keys()
                .into_iter()
                .map(serde_json::Value::String)
                .collect(),
        ),
        "dwbase.echo" => {
            let op_name = value
                .get("operation")
                .and_then(|v| v.as_str())
                .unwrap_or(operation);
            let input_text = value
                .get("input")
                .and_then(|v| v.as_str())
                .map(ToOwned::to_owned)
                .unwrap_or_else(|| value.to_string());
            serde_json::json!({
                "message": handle_message(op_name, &input_text)
            })
        }
        _ => qa::configure(&value),
    };
    encode_cbor(&output)
}
