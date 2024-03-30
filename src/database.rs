use std::{collections::HashMap, sync::RwLock};

use monoio::time::Instant;

enum Value {
    String(Vec<u8>),
}

struct Dataset {
    data: HashMap<Box<[u8]>, Value>,
    expiry: HashMap<Box<[u8]>, Instant>,
}

pub(crate) struct Database {
    config: RwLock<HashMap<Box<[u8]>, String>>,
    datasets: Vec<RwLock<Dataset>>
}

impl Database {
    pub(crate) fn new() -> Self {
        Self {
            config: RwLock::new(HashMap::new()),
            datasets: vec![RwLock::new(Dataset {
                data: HashMap::new(),
                expiry: HashMap::new(),
            })]
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
        let lock = self.datasets[0].read().unwrap();

        let value = match lock.data.get(key) {
            Some(value) => value,
            None => return None,
        };

        if let Some(expiry) = lock.expiry.get(key) {
            if expiry < &Instant::now() {
                return None;
            }
        }

        match value {
            Value::String(data) => Some(data.clone()),
        }
    }

    pub(crate) fn set(&self, key: &[u8], data: &[u8], expiry: Option<Instant>) {
        let key = key.to_vec().into_boxed_slice();
        let data = data.to_vec();

        let lock = &mut self.datasets[0].write().unwrap();

        if let Some(expiry) = expiry {
            lock.expiry.insert(key.clone(), expiry);
        } else {
            lock.expiry.remove(&key);
        }
        lock.data.insert(key, Value::String(data));
    }
}
