use std::{collections::HashMap, sync::RwLock};

pub(crate) struct Database {
    data: RwLock<HashMap<String, String>>,
}

impl Database {
    pub(crate) fn new() -> Self {
        Self {
            data: RwLock::new(HashMap::new()),
        }
    }

    pub(crate) fn get(&self, key: &str) -> Option<String> {
        self.data.read().unwrap().get(key).cloned()
    }

    pub(crate) fn set(&self, key: String, value: String) {
        self.data.write().unwrap().insert(key, value);
    }
}
