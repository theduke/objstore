use dioxus::prelude::*;

use crate::views::{BrowserPage as Browser, Home, NavbarLayout, NewConnection, Settings};

#[derive(Debug, Clone, Routable, PartialEq)]
#[rustfmt::skip]
pub enum Route {
    #[layout(NavbarLayout)]
        #[route("/")]
        Home {},

        #[route("/settings")]
        Settings {},

        #[route("/connections/new")]
        NewConnection {},

        #[route("/connections/:store/browser")]
        Browser {
            store: String,
        }
}
