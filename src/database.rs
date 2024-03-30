use std::{collections::HashMap, sync::RwLock};

use monoio::time::Instant;

struct Value {
    data: String,
    expiry: Option<Instant>,
}


pub(crate) struct Database {
    config: RwLock<HashMap<String, String>>,
    data: RwLock<HashMap<String, Value>>,
}

impl Database {
    pub(crate) fn new() -> Self {
        Self {
            config: RwLock::new(HashMap::new()),
            data: RwLock::new(HashMap::new()),
        }
    }

    pub(crate) fn get_config(&self, key: &str) -> Option<String> {
        self.config.read().unwrap().get(key).cloned()
    }

    pub(crate) fn set_config(&self, key: String, value: String) {
        self.config.write().unwrap().insert(key, value);
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
