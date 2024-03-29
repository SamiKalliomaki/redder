use std::{collections::HashMap, sync::RwLock};
use std::time::Instant;

struct Value {
    data: String,
    expiry: Option<Instant>,
}

pub(crate) struct Database {
    data: RwLock<HashMap<String, Value>>,
}

impl Database {
    pub(crate) fn new() -> Self {
        Self {
            data: RwLock::new(HashMap::new()),
        }
    }

    pub(crate) fn get(&self, key: &str) -> Option<String> {
        let lock = self.data.read().unwrap();
        let value = match lock.get(key) {
            Some(value) => value,
            None => return None,
        };

        if let Some(expiry) = value.expiry {
            if expiry < Instant::now() {
                return None;
            }
        }
        return Some(value.data.clone());
    }

    pub(crate) fn set(&self, key: String, data: String, expiry: Option<Instant>) {
        self.data.write().unwrap().insert(key, Value { data, expiry });
    }
}
