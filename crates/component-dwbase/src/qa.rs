use greentic_types::i18n_text::I18nText;
use greentic_types::schemas::component::v0_6_0::{QaMode, Question, QuestionKind};
use serde_json::{json, Map as JsonMap, Value as JsonValue};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NormalizedMode {
    Setup,
    Update,
    Remove,
}

impl NormalizedMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Setup => "setup",
            Self::Update => "update",
            Self::Remove => "remove",
        }
    }
}

pub fn normalize_mode(raw: &str) -> Option<NormalizedMode> {
    match raw {
        "default" | "setup" | "install" => Some(NormalizedMode::Setup),
        "update" | "upgrade" => Some(NormalizedMode::Update),
        "remove" => Some(NormalizedMode::Remove),
        _ => None,
    }
}

pub fn qa_spec_json(mode: NormalizedMode) -> JsonValue {
    let (title_key, description_key, questions) = match mode {
        NormalizedMode::Setup => (
            "qa.install.title",
            Some("qa.install.description"),
            vec![
                question(
                    "data_dir",
                    "qa.field.data_dir.label",
                    "qa.field.data_dir.help",
                    true,
                ),
                question(
                    "default_tenant",
                    "qa.field.default_tenant.label",
                    "qa.field.default_tenant.help",
                    true,
                ),
                question(
                    "public_base_url",
                    "qa.field.public_base_url.label",
                    "qa.field.public_base_url.help",
                    true,
                ),
                question(
                    "public_path_prefix",
                    "qa.field.public_path_prefix.label",
                    "qa.field.public_path_prefix.help",
                    false,
                ),
                question(
                    "nats_url",
                    "qa.field.nats_url.label",
                    "qa.field.nats_url.help",
                    false,
                ),
                question(
                    "swarm_enable",
                    "qa.field.swarm_enable.label",
                    "qa.field.swarm_enable.help",
                    false,
                ),
            ],
        ),
        NormalizedMode::Update => (
            "qa.update.title",
            Some("qa.update.description"),
            vec![
                question(
                    "data_dir",
                    "qa.field.data_dir.label",
                    "qa.field.data_dir.help",
                    false,
                ),
                question(
                    "default_tenant",
                    "qa.field.default_tenant.label",
                    "qa.field.default_tenant.help",
                    false,
                ),
                question(
                    "public_base_url",
                    "qa.field.public_base_url.label",
                    "qa.field.public_base_url.help",
                    false,
                ),
                question(
                    "public_path_prefix",
                    "qa.field.public_path_prefix.label",
                    "qa.field.public_path_prefix.help",
                    false,
                ),
                question(
                    "nats_url",
                    "qa.field.nats_url.label",
                    "qa.field.nats_url.help",
                    false,
                ),
                question(
                    "swarm_enable",
                    "qa.field.swarm_enable.label",
                    "qa.field.swarm_enable.help",
                    false,
                ),
            ],
        ),
        NormalizedMode::Remove => (
            "qa.remove.title",
            Some("qa.remove.description"),
            vec![question(
                "confirm_remove",
                "qa.field.confirm_remove.label",
                "qa.field.confirm_remove.help",
                true,
            )],
        ),
    };

    json!({
        "mode": match mode {
            NormalizedMode::Setup => QaMode::Setup,
            NormalizedMode::Update => QaMode::Update,
            NormalizedMode::Remove => QaMode::Remove,
        },
        "title": I18nText::new(title_key, None),
        "description": description_key.map(|key| I18nText::new(key, None)),
        "questions": questions,
        "defaults": {}
    })
}

fn question(id: &str, label_key: &str, help_key: &str, required: bool) -> Question {
    Question {
        id: id.to_string(),
        label: I18nText::new(label_key, None),
        help: Some(I18nText::new(help_key, None)),
        error: None,
        kind: QuestionKind::Text,
        required,
        default: None,
    }
}

pub fn i18n_keys() -> Vec<String> {
    crate::i18n::all_keys()
}

pub fn requirements_json() -> JsonValue {
    json!({
        "cap_id": "greentic.cap.dwbase.memory.v1",
        "provider_op": "dwbase.configure",
        "requires_http_ingress": true,
        "required_config_keys": ["data_dir", "default_tenant", "public_base_url"],
        "optional_config_keys": ["public_path_prefix", "nats_url", "swarm_enable"],
        "public_base_url_field": "public_base_url",
        "public_path_prefix_default": "/dwbase",
        "supports": ["component_config"],
        "secrets": []
    })
}

pub fn configure(payload: &JsonValue) -> JsonValue {
    let mode = payload
        .get("mode")
        .and_then(|value| value.as_str())
        .and_then(normalize_mode)
        .unwrap_or(NormalizedMode::Setup);
    let mut result = apply_answers(mode, payload);
    if let Some(map) = result.as_object_mut() {
        map.insert("requirements".to_string(), requirements_json());
    }
    result
}

pub fn apply_answers(mode: NormalizedMode, payload: &JsonValue) -> JsonValue {
    let answers = payload
        .get("answers")
        .cloned()
        .or_else(|| payload.get("config").cloned())
        .unwrap_or_else(|| json!({}));
    let current_config = payload
        .get("current_config")
        .cloned()
        .unwrap_or_else(|| json!({}));

    let mut errors = Vec::new();
    match mode {
        NormalizedMode::Setup => {
            for key in ["data_dir", "default_tenant", "public_base_url"] {
                if string_value(&answers, key).is_none() {
                    errors.push(json!({
                        "key": "qa.error.required",
                        "msg_key": "qa.error.required",
                        "fields": [key]
                    }));
                }
            }
        }
        NormalizedMode::Remove => {
            if !bool_value(&answers, "confirm_remove").unwrap_or(false) {
                errors.push(json!({
                    "key": "qa.error.remove_confirmation",
                    "msg_key": "qa.error.remove_confirmation",
                    "fields": ["confirm_remove"]
                }));
            }
        }
        NormalizedMode::Update => {}
    }

    validate_non_empty(
        &answers,
        "data_dir",
        "qa.error.invalid_data_dir",
        &mut errors,
    );
    validate_non_empty(
        &answers,
        "default_tenant",
        "qa.error.invalid_default_tenant",
        &mut errors,
    );
    validate_url(&answers, "public_base_url", &mut errors);
    validate_nats_url(&answers, &mut errors);

    if !errors.is_empty() {
        return json!({
            "ok": false,
            "warnings": [],
            "errors": errors,
            "meta": {
                "mode": mode.as_str(),
                "version": "v1"
            }
        });
    }

    let mut config = match current_config {
        JsonValue::Object(map) => map,
        _ => serde_json::Map::new(),
    };
    if let JsonValue::Object(answer_map) = answers {
        merge_answers(&mut config, &answer_map);
    }
    if mode != NormalizedMode::Remove {
        if !config.contains_key("public_path_prefix") {
            config.insert(
                "public_path_prefix".to_string(),
                JsonValue::String("/dwbase".to_string()),
            );
        }
        if !config.contains_key("swarm_enable") {
            config.insert("swarm_enable".to_string(), JsonValue::Bool(false));
        }
        config.insert(
            "ingress".to_string(),
            JsonValue::Object(build_ingress_config(&config)),
        );
        config.insert("enabled".to_string(), JsonValue::Bool(true));
    } else {
        config.insert("enabled".to_string(), JsonValue::Bool(false));
        config.insert("ingress_enabled".to_string(), JsonValue::Bool(false));
    }

    json!({
        "ok": true,
        "config": config,
        "warnings": [],
        "errors": [],
        "meta": {
            "mode": mode.as_str(),
            "version": "v1"
        },
        "audit": {
            "reasons": ["qa.apply_answers", "dwbase.configure"],
            "timings_ms": {}
        }
    })
}

fn merge_answers(config: &mut JsonMap<String, JsonValue>, answers: &JsonMap<String, JsonValue>) {
    for (key, value) in answers {
        match key.as_str() {
            "data_dir" | "default_tenant" | "public_base_url" | "public_path_prefix" => {
                if let Some(value) = string_json(value) {
                    config.insert(key.clone(), JsonValue::String(value));
                }
            }
            "nats_url" => {
                if let Some(value) = string_json(value) {
                    if value.is_empty() {
                        config.remove(key);
                    } else {
                        config.insert(key.clone(), JsonValue::String(value));
                    }
                }
            }
            "swarm_enable" | "confirm_remove" => {
                if let Some(value) = bool_json(value) {
                    config.insert(key.clone(), JsonValue::Bool(value));
                }
            }
            _ => {
                config.insert(key.clone(), value.clone());
            }
        }
    }
}

fn build_ingress_config(config: &JsonMap<String, JsonValue>) -> JsonMap<String, JsonValue> {
    let base_url = config
        .get("public_base_url")
        .and_then(JsonValue::as_str)
        .unwrap_or_default()
        .trim_end_matches('/')
        .to_string();
    let path_prefix = config
        .get("public_path_prefix")
        .and_then(JsonValue::as_str)
        .unwrap_or("/dwbase");
    let normalized_path = normalize_path_prefix(path_prefix);
    let public_api_base_url = if base_url.is_empty() {
        JsonValue::Null
    } else {
        JsonValue::String(format!("{base_url}{normalized_path}"))
    };

    JsonMap::from_iter([
        ("required".to_string(), JsonValue::Bool(true)),
        (
            "public_base_url".to_string(),
            config
                .get("public_base_url")
                .cloned()
                .unwrap_or(JsonValue::Null),
        ),
        (
            "public_path_prefix".to_string(),
            JsonValue::String(normalized_path),
        ),
        ("public_api_base_url".to_string(), public_api_base_url),
        ("ingress_enabled".to_string(), JsonValue::Bool(true)),
    ])
}

fn normalize_path_prefix(value: &str) -> String {
    let trimmed = value.trim();
    if trimmed.is_empty() || trimmed == "/" {
        return "/dwbase".to_string();
    }
    let mut normalized = trimmed.to_string();
    if !normalized.starts_with('/') {
        normalized.insert(0, '/');
    }
    normalized.trim_end_matches('/').to_string()
}

fn validate_non_empty(
    answers: &JsonValue,
    key: &str,
    error_key: &str,
    errors: &mut Vec<JsonValue>,
) {
    if let Some(value) = string_value(answers, key) {
        if value.trim().is_empty() {
            errors.push(json!({
                "key": error_key,
                "msg_key": error_key,
                "fields": [key]
            }));
        }
    }
}

fn validate_url(answers: &JsonValue, key: &str, errors: &mut Vec<JsonValue>) {
    if let Some(value) = string_value(answers, key) {
        let trimmed = value.trim();
        let valid = trimmed.starts_with("https://") || trimmed.starts_with("http://");
        if trimmed.is_empty() || !valid {
            errors.push(json!({
                "key": "qa.error.invalid_public_base_url",
                "msg_key": "qa.error.invalid_public_base_url",
                "fields": [key]
            }));
        }
    }
}

fn validate_nats_url(answers: &JsonValue, errors: &mut Vec<JsonValue>) {
    if let Some(value) = string_value(answers, "nats_url") {
        let trimmed = value.trim();
        if !(trimmed.is_empty()
            || trimmed.starts_with("nats://")
            || trimmed.starts_with("tls://")
            || trimmed.starts_with("ws://")
            || trimmed.starts_with("wss://"))
        {
            errors.push(json!({
                "key": "qa.error.invalid_nats_url",
                "msg_key": "qa.error.invalid_nats_url",
                "fields": ["nats_url"]
            }));
        }
    }
}

fn string_value<'a>(value: &'a JsonValue, key: &str) -> Option<&'a str> {
    value.get(key).and_then(JsonValue::as_str)
}

fn string_json(value: &JsonValue) -> Option<String> {
    value.as_str().map(|value| value.trim().to_string())
}

fn bool_value(value: &JsonValue, key: &str) -> Option<bool> {
    value.get(key).and_then(bool_json)
}

fn bool_json(value: &JsonValue) -> Option<bool> {
    match value {
        JsonValue::Bool(value) => Some(*value),
        JsonValue::String(value) => match value.trim().to_ascii_lowercase().as_str() {
            "true" | "1" | "yes" | "on" => Some(true),
            "false" | "0" | "no" | "off" => Some(false),
            _ => None,
        },
        _ => None,
    }
}
