//! Ensures `web-dist/` exists with at least a placeholder index.html so the
//! `rust-embed` in src/web.rs always compiles. `npm run build` in web/
//! overwrites the placeholder with the real SPA (gitignored).

fn main() {
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("web-dist");
    std::fs::create_dir_all(&dir).expect("creating web-dist");
    let index = dir.join("index.html");
    if !index.exists() {
        std::fs::write(&index, PLACEHOLDER).expect("writing placeholder web-dist/index.html");
    }
    println!("cargo:rerun-if-changed=web-dist");
}

const PLACEHOLDER: &str = r#"<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>burrow</title>
<style>
  body { font: 14px/1.6 ui-monospace, monospace; background: #0f1115; color: #d7dce3;
         display: grid; place-items: center; min-height: 100vh; margin: 0; }
  main { max-width: 52ch; padding: 2rem; }
  code { background: #1c212b; padding: .15em .4em; border-radius: 4px; }
</style>
</head>
<body><main>
<h1>burrow web UI</h1>
<p>The daemon's web server is running, but the frontend hasn't been built yet.</p>
<p>Build it with:</p>
<p><code>cd web &amp;&amp; npm install &amp;&amp; npm run build</code></p>
<p>then restart the daemon. The JSON API under <code>/api/v1/</code> works regardless.</p>
</main></body>
</html>
"#;
