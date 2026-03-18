use std::collections::BTreeMap;
use std::sync::OnceLock;

use crate::i18n_bundle::{unpack_locales_from_cbor, LocaleBundle};

include!(concat!(env!("OUT_DIR"), "/i18n_bundle.rs"));

static I18N_BUNDLE: OnceLock<LocaleBundle> = OnceLock::new();

fn bundle() -> &'static LocaleBundle {
    I18N_BUNDLE.get_or_init(|| unpack_locales_from_cbor(I18N_BUNDLE_CBOR).unwrap_or_default())
}

fn locale_chain(locale: &str) -> Vec<String> {
    let normalized = locale.replace('_', "-");
    let mut chain = vec![normalized.clone()];
    if let Some((base, _)) = normalized.split_once('-') {
        chain.push(base.to_string());
    }
    chain.push("en".to_string());
    chain
}

pub fn t(locale: &str, key: &str) -> String {
    for candidate in locale_chain(locale) {
        if let Some(map) = bundle().get(&candidate) {
            if let Some(value) = map.get(key) {
                return value.clone();
            }
        }
    }
    key.to_string()
}

pub fn all_keys() -> Vec<String> {
    let Some(en) = bundle().get("en") else {
        return Vec::new();
    };
    en.keys().cloned().collect()
}

pub fn en_messages() -> BTreeMap<String, String> {
    bundle().get("en").cloned().unwrap_or_default()
}
