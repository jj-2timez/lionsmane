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

    println!("\n=== Simulating Network Activity ===");

    // 4. Node A creates a new unique transaction transferring funds to Node B
    println!("[Node A] Generating a new signed transaction for 1500 tokens...");
    let tx = node_a.wallet.new_extrinsic(
        node_b_wallet_hex.clone(), // Receiver
        1500,                      // Amount
        "Transfer".to_string()     // Tx Type
    );

    // 5. Node A broadcasts the transaction over the P2P network using Iroh Gossip
    println!("[Node A] Broadcasting transaction {} to the P2P network...", tx.id);
    node_a.broadcast_transaction(&tx).await?;

    // 6. Wait to allow the background listener task to catch and process the replication message
    println!("\nWaiting for network propagation...");
    sleep(Duration::from_secs(3)).await;

    // 7. Verify that Node B safely updated its state cache
    let pool_guard = node_b.transaction_pool.lock().await;
    if pool_guard.transaction_pool.contains(&tx) {
        println!("\n[Success] Verification Complete: Node B has securely received and validated Node A's transaction in its MemPool!");
    } else {
        println!("\n[Notice] Transaction propagation incomplete or verification failed.");
    }

    Ok(())
}