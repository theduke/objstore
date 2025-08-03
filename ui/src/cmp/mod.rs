//! The components module contains all shared components for our app. Components are the building blocks of dioxus apps.
//! They can be used to defined common UI elements like buttons, forms, and modals. In this template, we define a Hero
//! component  to be used in our app.

pub mod util;

pub mod s3;

mod browser;
pub use browser::Browser;

mod connection_manager;
pub use connection_manager::ConnectionManager;

mod object_delete_modal;
use object_delete_modal::ObjectDeleteModal;

mod object;
