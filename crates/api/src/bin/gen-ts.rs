//! Writes `packages/api/src/generated.ts`.

fn main() -> std::io::Result<()> {
    let path = concat!(env!("CARGO_MANIFEST_DIR"), "/../../packages/api/src/generated.ts");
    std::fs::write(path, traumfaenger_api::typescript_bindings())?;
    println!("wrote {path}");
    Ok(())
}
