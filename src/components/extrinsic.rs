use std::collections::HashSet;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Extrinsic {
    pub id: String,
    pub sender_public_key: String,
    pub receiver_public_key: String,
    pub amount: u64,
    pub transaction_type: String,
    pub timestamp: u64,
    pub signature: String,
}

impl Extrinsic {
    pub fn hash_data(&self) -> blake3::Hash {
        let mut clone = self.clone();
        clone.signature = String::new(); // Clear signature before hashing

        let bytes = bincode::serialize(&clone).expect("Failed to serialize extrinsic");
        blake3::hash(&bytes)
    }
}

#[derive(Debug, Default)]
pub struct MemPool {
    pub transaction_pool: HashSet<Extrinsic>,
}

impl MemPool {
    pub fn new() -> Self {
        Self {
            transaction_pool: HashSet::new(),
        }
    }
    pub fn add_extrinsic(&mut self, tx: Extrinsic) {
        self.transaction_pool.insert(tx);
    }
    pub fn pop_extrinsic(&mut self, new_block_transactions: &[Extrinsic]) {
        // Build a temporary set of items to remove for instant O(1) lookups
        let to_remove: HashSet<&Extrinsic> = new_block_transactions.iter().collect();
        // Retain only the transactions that were NOT included in the new block
        self.transaction_pool.retain(|tx| !to_remove.contains(tx));
    }
}


