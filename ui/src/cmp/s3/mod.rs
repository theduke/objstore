mod form;
pub use self::form::S3Form;

pub enum ConnectionPersistence {
    Temporary,
    Persistent,
}
