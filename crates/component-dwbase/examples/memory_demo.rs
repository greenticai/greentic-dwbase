use component_dwbase::qa::{configure, requirements_json, NormalizedMode};
use serde_json::json;

fn main() {
    let data_dir = std::env::var("DWBASE_DATA_DIR").unwrap_or_else(|_| ".dwbase".into());
    let default_tenant = std::env::var("DWBASE_TENANT_ID").unwrap_or_else(|_| "default".into());
    let public_base_url = std::env::var("DWBASE_PUBLIC_BASE_URL")
        .unwrap_or_else(|_| "https://example.invalid".into());
    let public_path_prefix =
        std::env::var("DWBASE_PUBLIC_PATH_PREFIX").unwrap_or_else(|_| "/dwbase".into());

    let payload = json!({
        "mode": NormalizedMode::Setup.as_str(),
        "answers": {
            "data_dir": data_dir,
            "default_tenant": default_tenant,
            "public_base_url": public_base_url,
            "public_path_prefix": public_path_prefix,
            "swarm_enable": false
        }
    });

    let configured = configure(&payload);
    println!("requirements={}", requirements_json());
    println!("configured={configured}");
}
