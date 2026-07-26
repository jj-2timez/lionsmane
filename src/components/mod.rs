mod extrinsic;
mod ledger;
mod utils;
use tokio_stream::StreamExt;
use getrandom::{SysRng, rand_core::UnwrapErr};
use ed25519_dalek::{SigningKey, VerifyingKey, Signature, Signer, Verifier};
use hex;
use uuid::Uuid;
use serde::{Deserialize, Serialize};
use iroh::{Endpoint, EndpointAddr, protocol::Router, endpoint::presets};
use std::sync::Arc;
use tokio::sync::Mutex;
use iroh_gossip::{ Gossip, TopicId,api::{Event, GossipReceiver, GossipSender},
};
#[derive(Serialize, Deserialize, Debug)]
pub enum NetworkMessage {
    NewTransaction(extrinsic::Extrinsic),
    NewBlock(ledger::Block),
}

pub struct Node {
    pub transaction_pool: Arc<Mutex<extrinsic::MemPool>>,
    pub wallet: Wallet,
    pub ledger: Arc<Mutex<ledger::Ledger>>,
    pub endpoint: Endpoint,
    pub gossip: Gossip,
    pub gossip_sender: GossipSender,
    pub router: Router, 
}

impl Node {
    pub async fn new(blockchain_topic_bytes: [u8; 32],bootstrap_peers: Vec<EndpointAddr>,) -> anyhow::Result<Self> {
        let transaction_pool = Arc::new(Mutex::new(extrinsic::MemPool::new()));
        let wallet = Wallet::new();
        let ledger = Arc::new(Mutex::new(ledger::Ledger::new()));

        let endpoint = Endpoint::bind(presets::N0).await?;

        let gossip = Gossip::builder().spawn(endpoint.clone());

        // THE FIX: Bind to a proper variable (no underscore) so we can return it.
        let router = Router::builder(endpoint.clone())
            .accept(iroh_gossip::ALPN, gossip.clone())
            .spawn();

        let topic_id = TopicId::from_bytes(blockchain_topic_bytes);
        let mut seed_peer_ids = Vec::new();

        for peer_addr in bootstrap_peers {
            seed_peer_ids.push(peer_addr.id);
            
            // Explicitly dial the raw IPs embedded in the EndpointAddr.
            if let Ok(conn) = endpoint.connect(peer_addr.clone(), iroh_gossip::ALPN).await {
                // THE FIX: Pass the connection handle to a detached async task.
                // This keeps the QUIC tunnel alive in the background for 15 seconds, 
                // giving Gossip plenty of time to lock onto the route and handshake.
                tokio::spawn(async move {
                    tokio::time::sleep(std::time::Duration::from_secs(15)).await;
                    drop(conn);
                });
            }
        }

        let (gossip_sender, gossip_receiver) = gossip
            .subscribe(topic_id, seed_peer_ids)
            .await?
            .split();

        let node = Self {
            transaction_pool: transaction_pool.clone(),
            wallet,
            ledger: ledger.clone(),
            endpoint,
            gossip,
            gossip_sender,
            router, // Safely pass the router into the Node struct
        };

        // Spawn the background network listener
        tokio::spawn(Self::listen_to_network(gossip_receiver, transaction_pool, ledger));

        Ok(node)
    }

    /// Broadcasts a newly signed Block to the entire P2P network
    pub async fn broadcast_block(&self, block: &ledger::Block) -> anyhow::Result<()> {
        let serialized_msg = NetworkMessage::NewBlock(block.clone());
        let payload = bincode::serialize(&serialized_msg)?;
        
        self.gossip_sender.broadcast(payload.into()).await?;
        Ok(())
    }

    /// Broadcasts a transaction to the network mempool
    pub async fn broadcast_transaction(&self, tx: &extrinsic::Extrinsic) -> anyhow::Result<()> {
        let serialized_msg = NetworkMessage::NewTransaction(tx.clone());
        let payload = bincode::serialize(&serialized_msg)?;
        
        self.gossip_sender.broadcast(payload.into()).await?;
        Ok(())
    }

    /// The continuous processing loop for handling incoming network traffic
    async fn listen_to_network(
        mut receiver: GossipReceiver,
        pool: Arc<Mutex<extrinsic::MemPool>>,
        ledger: Arc<Mutex<ledger::Ledger>>,
    ) {
        while let Some(result) = receiver.next().await {
            match result {
                Ok(event) => match event {
                    Event::NeighborUp(peer) => {
                        println!("🔗 [Network] P2P Mesh Link Established with: {}", peer);
                    }
                    Event::NeighborDown(peer) => {
                        println!("⚠️ [Network] P2P Mesh Link Lost with: {}", peer);
                    }
                    Event::Received(msg) => {
                        if let Ok(network_msg) = bincode::deserialize::<NetworkMessage>(&msg.content) {
                            match network_msg {
                                NetworkMessage::NewTransaction(tx) => {
                                    println!("✅ [Network] Received transaction via P2P: {}", tx.id);
                                    let mut pool_guard = pool.lock().await;
                                    pool_guard.add_extrinsic(tx);
                                }
                                NetworkMessage::NewBlock(block) => {
                                    println!("✅ [Network] Received block via P2P count: {}", block.block_count);
                                    let mut ledger_guard = ledger.lock().await;
                                    if ledger_guard.validate_block_count(&block) 
                                        && ledger_guard.validate_previous_block_hash(&block) 
                                    {
                                        ledger_guard.add_block(block.clone());
                                        let mut pool_guard = pool.lock().await;
                                        pool_guard.pop_extrinsic(&block.transactions);
                                    }
                                }
                            }
                        }
                    }
                    _ => {}
                },
                Err(e) => {
                    println!("❌ [Network] Gossip stream error: {:?}", e);
                }
            }
        }
    }
}
pub struct Wallet {
    key_pair: SigningKey,
}

impl Wallet {
   pub fn new() -> Self {
        let mut csprng = UnwrapErr(SysRng);
        let key_pair = SigningKey::generate(&mut csprng);
        Self { key_pair }
    }

    pub fn public_key_hex(&self) -> String {
        hex::encode(self.key_pair.verifying_key().to_bytes())
    }

    pub fn sign_hash(&self, hash: &blake3::Hash) -> String {
        let signature = self.key_pair.sign(hash.as_bytes());
        hex::encode(signature.to_bytes())
    }

    pub fn validate_signature(
        signature_hex: &str,
        hash: &blake3::Hash,
        public_key_hex: &str,
    ) -> Result<(), String> {
        let pubkey_bytes = hex::decode(public_key_hex).map_err(|e| e.to_string())?;
        let public_key = VerifyingKey::try_from(pubkey_bytes.as_slice()).map_err(|e| e.to_string())?;

        let sig_bytes = hex::decode(signature_hex).map_err(|e| e.to_string())?;
        let signature = Signature::from_slice(&sig_bytes).map_err(|e| e.to_string())?;

        match public_key.verify(hash.as_bytes(), &signature) {
            Ok(_) => Ok(()),
            Err(e) => Err(e.to_string()),
        }
    }

    pub fn new_extrinsic(&self,receiver_public_key: String, amount: u64, transaction_type: String) -> extrinsic::Extrinsic {
        let mut tx = extrinsic::Extrinsic {
            id: Uuid::new_v4().simple().to_string(),
            sender_public_key: self.public_key_hex(),
            receiver_public_key,
            amount,
            transaction_type,
            timestamp: utils::current_timestamp(),
            signature: String::new(),
        };

        tx.signature = self.sign_hash(&tx.hash_data());
        tx
    }

    pub fn create_block(&self, transactions: Vec<extrinsic::Extrinsic>, previous_hash: String, block_count: u64,) -> ledger::Block {
        let mut block = ledger::Block {
            transactions,
            previous_hash,
            signer: self.public_key_hex(),
            block_count,
            timestamp: utils::current_timestamp(),
            signature: String::new(),
        };

        block.signature = self.sign_hash(&block.hash_data());
        block
    }
}
