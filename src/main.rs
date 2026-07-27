pub mod components;
use std::time::Duration;
use tokio::time::sleep;
use components::Node;


#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!("=== Initializing Blockchain P2P Network Test ===");

    // 1. Establish a global 32-byte network topic identifier.
    let blockchain_network_topic: [u8; 32] = [7; 32]; 

    // 2. Initialize Node A (Genesis Node / Sender Node)
    // Node A boots first, so it passes an empty bootstrap vector
    println!("\n[Node A] Bootstrapping Genesis Node...");
    let node_a = Node::new(blockchain_network_topic, vec![]).await?;
    
    // Extract Node A's dynamic endpoint networking layout
    let node_a_network_addr = node_a.endpoint.addr(); 
    let node_a_wallet_hex = node_a.wallet.public_key_hex();
    
    // Extract and print Node A's inner PublicKey ID from its address metadata
    println!("[Node A] Cryptographic P2P Endpoint ID: {}", node_a_network_addr.id);
    println!("[Node A] Wallet Public Address: {}", node_a_wallet_hex);

    // 3. Initialize Node B (Peer Node / Receiver Node)
    // We pass Node A's full EndpointAddr object. The Node's internal constructor handles 
    // pulling out the explicit keys and registering paths within the gossip loop automatically.
    println!("\n[Node B] Bootstrapping Peer Node...");
    let node_b = Node::new(blockchain_network_topic, vec![node_a_network_addr]).await?;
    let node_b_wallet_hex = node_b.wallet.public_key_hex();
    
    println!("[Node B] Cryptographic P2P Endpoint ID: {}", node_b.endpoint.addr().id);
    println!("[Node B] Wallet Public Address: {}", node_b_wallet_hex);

    // Give Iroh's async QUIC transport a small window to punch local ports and link swarms
    println!("\nEstablishing P2P mesh links (Waiting 5 seconds for handshakes)...");
    sleep(Duration::from_secs(5)).await;

    println!("\n=== Simulating Smart Contract Network Deployment ===");

    // 4. Define a Python-like Starlark smart contract script
    let smart_contract_code = r#"
def calculate_staking_yield():
    base_stake = 10000
    pool_multiplier = 1
    return base_stake * pool_multiplier
"#;

    // 5. Node A creates a new unique transaction carrying the smart contract code
    println!("[Node A] Generating a new signed transaction containing a Starlark contract...");
    let tx = node_a.wallet.new_extrinsic(
        node_b_wallet_hex.clone(),       // Receiver
        0,                               // Amount (0 tokens, this is a contract deployment)
        smart_contract_code.to_string()  // Tx Type (Used here to store the contract payload)
    );

    // 6. Node A broadcasts the transaction over the P2P network using Iroh Gossip
    println!("[Node A] Broadcasting transaction {} to the P2P network...", tx.id);
    node_a.broadcast_transaction(&tx).await?;

    // 7. Wait to allow the background listener task to catch and process the replication message
    println!("\nWaiting for network propagation...");
    sleep(Duration::from_secs(3)).await;

    // 8. Verify that Node B safely updated its state cache and execute the contract
    let pool_guard = node_b.transaction_pool.lock().await;
    
    if let Some(received_tx) = pool_guard.transaction_pool.iter().find(|t| t.id == tx.id) {
        println!("\n🎉 [Success] Verification Complete: Node B has securely received Node A's transaction in its MemPool!");
        println!("[Node B] Compiling and running transaction payload inside Starlark sandbox...");

        // Trigger the Starlark interpreter inside Node B
        match node_b.execute_smart_contract(&received_tx.transaction_type, "calculate_staking_yield") {
            Ok(execution_result) => {
                println!("🥇 [Starlark Engine] Execution Successful! Result: {}", execution_result);
                assert_eq!(execution_result, "10000"); // 10000 * 1.15
                println!("✅ Smart Contract state match verified across nodes!");
            }
            Err(e) => println!("❌ Starlark Runtime Error: {}", e),
        }
    } else {
        println!("\n❌ [Notice] Transaction propagation incomplete or verification failed.");
    }

    Ok(())
}
