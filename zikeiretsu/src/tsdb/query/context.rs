pub struct DBContext {
    pub db_dir: String,
}

impl DBContext {
    pub fn new(db_dir: String) -> Self {
        Self { db_dir }
    }
}
