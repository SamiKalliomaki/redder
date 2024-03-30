use std::{collections::HashMap, sync::RwLock};

use monoio::time::Instant;

struct Value {
    data: Vec<u8>,
    expiry: Option<Instant>,
}


pub(crate) struct Database {
    config: RwLock<HashMap<Box<[u8]>, String>>,
    data: RwLock<HashMap<Box<[u8]>, Value>>,
}

impl Database {
    pub(crate) fn new() -> Self {
        Self {
            config: RwLock::new(HashMap::new()),
            data: RwLock::new(HashMap::new()),
        }
    }

    pub(crate) fn get_config(&self, key: &[u8]) -> Option<String> {
        self.config.read().unwrap().get(key).cloned()
    }

    pub(crate) fn set_config(&self, key: &[u8], value: String) {
        let key = key.to_vec().into_boxed_slice();
        self.config.write().unwrap().insert(key, value);
    }

    pub(crate) fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
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

    pub(crate) fn set(&self, key: &[u8], data: &[u8], expiry: Option<Instant>) {
        let key = key.to_vec().into_boxed_slice();
        let data = data.to_vec();
        self.data.write().unwrap().insert(key, Value { data, expiry });
    }
}
