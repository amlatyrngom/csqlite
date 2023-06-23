#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // TODO: Replace with "deployment.toml" when done testing.
    let main_deployment = include_str!("deployment.toml");
    let deployments: Vec<String> = vec![main_deployment.into()];
    obelisk_deployment::build_user_deployment("csqlite", &deployments).await;
    // Creates base actor.
    let _fc = obelisk::FunctionalClient::new("csqlite", "dbactor", Some(0), None).await;
    Ok(())
}
