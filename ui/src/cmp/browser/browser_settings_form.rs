use dioxus::prelude::*;

/// Settings form for the browser component.
#[component]
pub fn BrowserSettingsForm(
    initial_pagination: bool,
    initial_pagination_size: u64,
    on_change_pagination: EventHandler<bool>,
    on_change_pagination_size: EventHandler<u64>,
    on_close: EventHandler<()>,
) -> Element {
    // Local state for the pagination checkbox and page size
    let mut pagination = use_signal(|| initial_pagination);
    let mut pagination_size = use_signal(|| initial_pagination_size);

    rsx! {
        div {
            class: "field",
            label {
                class: "checkbox",
                input {
                    type: "checkbox",
                    checked: "{pagination}",
                    onchange: move |e| {
                        pagination.set(e.checked());
                        on_change_pagination.call(e.checked());
                    },
                }
                " Pagination"
            }
            span {
                class: "help",
                "If enabled, you need to manually paginate, otherwise it will incrementally load all items in the current \"directory\"."
            }
        }

        div {
            class: "field",
            label {
                class: "label",
                "Pagination size"
            }
            div {
                class: "control",
                input {
                    class: "input",
                    type: "number",
                    value: "{pagination_size}",
                    min: "1",
                    max: "1000",
                    onchange: move |e| {
                        let mut val = e.value().parse::<u64>().unwrap_or(1);
                        val = val.clamp(1, 1000);
                        pagination_size.set(val);
                        on_change_pagination_size.call(val);
                    },
                }
            }
            span {
                class: "help",
                "Number of items per page (1-1000)."
            }
        }

        div {
            class: "buttons",

            button {
                class: "button",
                onclick: move |_| {
                    on_close.call(());
                },
                "Close"
            }
        }
    }
}
