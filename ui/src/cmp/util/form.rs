use dioxus::prelude::*;

pub enum FormSubmit {
    Idle,
    Loading,
    Error(String),
}

impl FormSubmit {
    /// Returns `true` if the form submit is [`Loading`].
    ///
    /// [`Loading`]: FormSubmit::Loading
    #[must_use]
    pub fn is_loading(&self) -> bool {
        matches!(self, Self::Loading)
    }
}

pub fn FormField(label: Element, children: Element) -> Element {
    rsx! {
        div {
            class: "field",
            label {
                class: "label",
                {label}
            }

            {children}
        }
    }
}
