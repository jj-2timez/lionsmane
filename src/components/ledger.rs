use std::collections::HashMap;
use serde::{Deserialize, Serialize};
use crate::components::extrinsic::Extrinsic;
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Block {
    pub transactions: Vec<Extrinsic>, // Using Vec instead of HashSet for deterministic serialization
    pub previous_hash: String,
    pub signer: String,
    pub block_count: u64,
    pub timestamp: u64,
    pub signature: String,
}

impl Block {
    /// Clears the signature, serializes with bincode, and hashes with BLAKE3
    pub fn hash_data(&self) -> blake3::Hash {
        let mut clone = self.clone();
        clone.signature = String::new(); // Clear signature before hashing

        // Serialize using Bincode
        let bytes = bincode::serialize(&clone).expect("Failed to serialize block");
        blake3::hash(&bytes)
    }
}

#[derive(Debug, Default, Clone)]
pub struct Accounts {
    balances: HashMap<String, u64>,
}

impl Accounts {
    pub fn new() -> Self {
        Self {
            balances: HashMap::new(),
        }
    }
    pub fn add_account(&mut self, public_key: String) {
        self.balances.entry(public_key).or_insert(0);
    }

    pub fn get_balance(&mut self, public_key: &str) -> u64 {
        *self.balances.entry(public_key.to_string()).or_insert(0)
    }
    pub fn add_balance(&mut self, public_key: &str, amount: u64) {
        let balance = self.balances.entry(public_key.to_string()).or_insert(0);
        *balance += amount;
    }
    pub fn sub_balance(&mut self, public_key: &str, amount: u64) -> Result<(), String> {
        let balance = self.balances.entry(public_key.to_string()).or_insert(0);
        if *balance >= amount {
            *balance -= amount;
            Ok(())
        } else {
            Err(format!("Insufficient funds for account: {}", public_key))
        }
    }
}


#[derive(Debug, Default)]
pub struct Ledger {
    pub blocks: Vec<Block>,
    pub accounts: Accounts,
}

impl Ledger {
    pub fn new() -> Self {
        Self {
            blocks: Vec::new(),
            accounts: Accounts::new(),
        }
    }

    pub fn add_block(&mut self, block: Block) {
        self.push_extrinsics(&block.transactions);
        self.blocks.push(block);
    }

    pub fn validate_block_count(&self, block: &Block) -> bool {
        match self.blocks.last() {
            Some(last_block) => last_block.block_count == block.block_count - 1,
            None => block.block_count == 0, // Handle Genesis block
        }
    }

    pub fn validate_previous_block_hash(&self, block: &Block) -> bool {
        match self.blocks.last() {
            Some(last_block) => {
                // Hash the last block and convert it to a hex string for comparison
                let current_hash = last_block.hash_data().to_hex().to_string();
                current_hash == block.previous_hash
            }
            None => block.previous_hash.is_empty(), // Genesis block has no previous hash
        }
    }

    /// Filters and returns a vector of transactions that the sender can afford.
    pub fn extrinsic_set_covered(&mut self, transactions: &[Extrinsic]) -> Vec<Extrinsic> {
        let mut covered_transactions = Vec::new();

        for tx in transactions {
            if self.accounts.get_balance(&tx.sender_public_key) >= tx.amount {
                covered_transactions.push(tx.clone());
            } else {
                println!("Transaction not covered by sender: {}", tx.sender_public_key);
            }
        }

        covered_transactions
    }

    pub fn push_extrinsics(&mut self, transactions: &[Extrinsic]) {
        for tx in transactions {
            // Attempt to deduct the sender's balance. 
            // If it fails (insufficient funds), skip to the next transaction.
            if let Err(e) = self.accounts.sub_balance(&tx.sender_public_key, tx.amount) {
                println!("Failed to process tx: {}", e);
                continue; 
            }
            
            // If deduction succeeded, credit the receiver.
            self.accounts.add_balance(&tx.receiver_public_key, tx.amount);
        }
    }
}