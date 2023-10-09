#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // TODO: Replace with "deployment.toml" when done testing.
    let main_deployment = include_str!("deployment.toml");
    let deployments: Vec<String> = vec![main_deployment.into()];
    obelisk_deployment::build_user_deployment("csqlite", &deployments).await;
    // Creates base actor.
    // let fc1 = obelisk::FunctionalClient::new("csqlite", "dbactor", Some(0), None);
    let fc2 = obelisk::FunctionalClient::new("csqlite", "rkvactor", Some(0), None);
    let _fcs = tokio::join!(fc2);
    Ok(())
}
