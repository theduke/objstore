mod browser_settings_form;
mod table;

use std::sync::Arc;

use dioxus::{core::Task, prelude::*};
use dioxus_bulma::Modal;
use futures::StreamExt as _;
use objstore::{ListArgs, ObjectMeta};

use crate::{
    cmp::{
        object::{
            download_modal::DownloadModal, object_creator::ObjectCreator, viewer::ObjectViewer,
        },
        object_delete_modal::ObjectDeleteModal,
        util::loader::{LoadState, Spinner},
    },
    context::ActiveStore,
};

use browser_settings_form::BrowserSettingsForm;
use table::ObjectsTable;

#[derive(Clone, Debug, PartialEq, Eq)]
enum ModalView {
    DeleteObject { meta: Arc<ObjectMeta> },
    DownloadObject { meta: Arc<ObjectMeta> },
    ViewObject { meta: Arc<ObjectMeta> },
    CreateObject { base_path: String },
}

enum Msg {
    GotoPath(String),
    Download(Arc<ObjectMeta>),
    DeleteObject(Arc<ObjectMeta>),
    ViewObject(Arc<ObjectMeta>),
    ObjectDeleted { key: String },
    CreateObject { base_path: String },
    ObjectCreated { meta: Arc<ObjectMeta> },
    LoadMore,
}

#[derive(Default)]
struct Page {
    objects: Vec<Arc<ObjectMeta>>,
    prefixes: Option<Vec<String>>,
    next_cursor: Option<String>,
}

/// Object store browser.
#[component]
pub fn Browser(store: ActiveStore) -> Element {
    let mut next_cursor = use_signal::<Option<String>>(|| None);

    let mut path = use_signal(|| String::new());
    let mut load_state = use_signal::<LoadState<()>>(|| LoadState::Idle);
    let mut page = use_signal::<Page>(|| Page::default());
    let mut modal_view = use_signal::<Option<ModalView>>(|| None);
    let mut manual_pagination = use_signal(|| true);
    let mut pagination_size = use_signal(|| 250u64);
    let mut show_settings = use_signal(|| false);

    let tx = use_coroutine::<Msg, _, _>({
        let store = store.store.clone();
        move |mut rx| {
            let store = store.clone();
            async move {
                let mut task: Option<Task> = None;

                let mut load = {
                    let store = store.clone();
                    move |args: ListArgs, extend: bool| {
                        let args = args.with_limit(pagination_size());
                        if let Some(t) = task.take() {
                            t.cancel();
                        }

                        path.set(args.prefix().map(|p| p.to_owned()).unwrap_or_default());

                        let store = store.clone();
                        let f = spawn(async move {
                            let args = args.with_delimiter('/');

                            tracing::info!("Loading page with args: {args:?}");
                            load_state.set(LoadState::Loading);
                            let res = store.list(args).await;
                            match res {
                                Ok(new_page) => {
                                    tracing::info!(
                                        "Loaded page, items: {}, prefixes: {}",
                                        new_page.items.len(),
                                        new_page.prefixes.as_ref().map_or(0, |p| p.len())
                                    );

                                    next_cursor.set(new_page.next_cursor.clone());
                                    let objects =
                                        new_page.items.into_iter().map(|meta| Arc::new(meta));

                                    if extend {
                                        let mut old_page = page.write_unchecked();
                                        old_page.objects.extend(objects);
                                        old_page.next_cursor = new_page.next_cursor;
                                    } else {
                                        let new_page = Page {
                                            objects: objects.collect(),
                                            prefixes: new_page.prefixes,
                                            next_cursor: new_page.next_cursor.clone(),
                                        };
                                        page.set(new_page);
                                    }

                                    load_state.set(LoadState::Loaded(Ok(())));
                                }
                                Err(err) => {
                                    tracing::error!("Error loading page: {err}");
                                    load_state.set(LoadState::Loaded(Err(err.to_string())));
                                }
                            };
                        });
                        task = Some(f);
                    }
                };

                // TODO: respect URL path!
                load(ListArgs::new().with_limit(pagination_size()), false);

                while let Some(msg) = rx.next().await {
                    match msg {
                        Msg::DeleteObject(meta) => {
                            modal_view.set(Some(ModalView::DeleteObject { meta }));
                        }
                        Msg::ObjectDeleted { key } => {
                            let mut page = page.write_unchecked();
                            page.objects.retain(|item| item.key != key);
                        }
                        Msg::GotoPath(mut path) => {
                            if !path.ends_with('/') {
                                path.push('/');
                            }
                            let args = ListArgs::new()
                                .with_prefix(path)
                                .with_limit(pagination_size());
                            load(args, false)
                        }
                        Msg::Download(meta) => {
                            modal_view.set(Some(ModalView::DownloadObject { meta }));
                        }
                        Msg::ViewObject(meta) => {
                            modal_view.set(Some(ModalView::ViewObject { meta }));
                        }
                        Msg::CreateObject { base_path } => {
                            modal_view.set(Some(ModalView::CreateObject { base_path }));
                        }
                        Msg::LoadMore => {
                            if let Some(cursor) = { page.read_unchecked().next_cursor.clone() } {
                                let prefix = path.read_unchecked().clone();
                                let args = ListArgs::new()
                                    .with_prefix(prefix)
                                    .with_cursor(cursor)
                                    .with_limit(pagination_size());
                                load(args, true)
                            }
                        }
                        Msg::ObjectCreated { meta } => {
                            let mut page = page.write_unchecked();
                            let path = path.read_unchecked().clone();
                            let key = meta.key.clone();
                            if key.starts_with(&path) {
                                let rest = &key[path.len()..];
                                if !rest.contains('/') {
                                    page.objects.push(meta.clone());
                                } else if let Some(prefixes) = page.prefixes.as_mut() {
                                    let next = rest.split_once('/').unwrap().0;
                                    let full = format!("{}{}/", path, next);
                                    if !prefixes.contains(&full) {
                                        prefixes.push(full);
                                        prefixes.sort();
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    });

    let modal = use_memo(move || {
        let Some(view) = modal_view() else {
            return VNode::empty();
        };

        match view {
            ModalView::DeleteObject { meta } => {
                rsx! {
                    ObjectDeleteModal {
                        store: store.store.clone(),
                        object_key: meta.key.clone(),
                        on_complete: {
                            let key = meta.key.clone();
                            move || {
                                modal_view.set(None);
                                tx.send(Msg::ObjectDeleted {
                                    key: key.clone(),
                                });
                            }
                        },
                        on_cancel: move || {
                            modal_view.set(None);
                        },
                    }
                }
            }
            ModalView::DownloadObject { meta } => {
                rsx! {
                    DownloadModal {
                        store: store.store.clone(),
                        object_meta: meta.clone(),
                        on_complete: move || {
                            modal_view.set(None);
                        },
                        on_cancel: move || {
                            modal_view.set(None);
                        },
                    }
                }
            }
            ModalView::ViewObject { meta } => {
                rsx! {
                    Modal {
                        on_close: move || {
                            modal_view.set(None);
                        },
                        ObjectViewer {
                            meta: meta.clone(),
                        }
                    }
                }
            }
            ModalView::CreateObject { base_path } => {
                rsx! {
                    Modal {
                        on_close: move || {
                            modal_view.set(None);
                        },
                        ObjectCreator {
                            store: store.store.clone(),
                            base_path: base_path.clone(),
                            on_complete: move |meta| {
                                modal_view.set(None);
                                tx.send(Msg::ObjectCreated { meta });
                            },
                            on_cancel: move || {
                                modal_view.set(None);
                            },
                        }
                    }
                }
            }
        }
    });

    let breadcrumbs = use_memo(move || {
        tracing::info!("Rendering breadcrumb for path: {}", path());

        let mut items = vec![("<root>".to_string(), "".to_string())];

        let path = path();
        if !path.is_empty() {
            let mut full_path = String::new();
            let parts = path.trim_end_matches('/').split('/').map(|segment| {
                full_path.push_str(segment);
                if !full_path.is_empty() {
                    full_path.push('/');
                }
                (segment.to_string(), full_path.clone())
            });
            items.extend(parts);
        }

        rsx! {
            nav {
                class: "box breadcrumb mb-4",
                aria_label: "breadcrumbs",

                ul {
                    for (segment, full_path) in items {
                        li {
                            a {
                                onclick: move |_| {
                                    tx.send(Msg::GotoPath(full_path.clone()));
                                },
                                "{segment}"
                            }
                        }
                    }
                }
            }
        }
    });

    let now = time::OffsetDateTime::now_utc();

    let action_bar = rsx! {

        div {
            class: "buttons mt-2 mb-2",

            button {
                class: "button mb-2 ml-2",
                title: "Refresh",
                aria_label: "Refresh",
                onclick: {
                    let current = path.read_unchecked().clone();
                    move |_| {
                        tx.send(Msg::GotoPath(current.clone()));
                    }
                },
                dioxus_free_icons::Icon {
                    fill: "black",
                    width: 20,
                    height: 20,
                    icon: dioxus_free_icons::icons::fa_solid_icons::FaArrowsRotate,
                }
            }

            button {
                class: "button mb-2",
                title: "Create Object",
                aria_label: "Create Object",
                onclick: {
                    let base = path.read_unchecked().clone();
                    move |_| {
                        tx.send(Msg::CreateObject { base_path: base.clone() });
                    }
                },
                dioxus_free_icons::Icon {
                    fill: "black",
                    width: 20,
                    height: 20,
                    icon: dioxus_free_icons::icons::fa_solid_icons::FaFileCirclePlus,
                }
            }

            button {
                class: if show_settings() { "button mb-2 is-active" } else { "button mb-2" },
                title: "Settings",
                aria_label: "Settings",
                onclick: move |_| show_settings.set(!show_settings()),
                dioxus_free_icons::Icon {
                    fill: "black",
                    width: 20,
                    height: 20,
                    icon: dioxus_free_icons::icons::fa_solid_icons::FaGear,
                }
            }
        }

    };

    let contents = {
        match &*load_state.read() {
            LoadState::Loading => {
                rsx! {
                    div {
                        Spinner {}
                    }
                }
            }
            LoadState::Idle => VNode::empty(),
            LoadState::Loaded(Ok(())) => {
                let page_data = page.read();

                rsx! {
                    if let Some(prefixes) = page_data.prefixes.as_ref().filter(|p| !p.is_empty()) {
                        div {
                            for prefix in prefixes {
                                button {
                                    class: "button mb-1",
                                    display: "block",
                                    onclick: {
                                        let prefix = prefix.clone();
                                        move |_| {
                                            tx.send(Msg::GotoPath(prefix.clone()));
                                        }
                                    },

                                    {
                                        let prefix = prefix.trim_end_matches('/');
                                        let name = if let Some((_, name)) = prefix.rsplit_once('/') {
                                            name
                                        } else {
                                            prefix
                                        };
                                        name.to_string()
                                    }
                                }

                            }
                        }

                        hr {}
                    }

                    if page_data.objects.is_empty() {
                        div {
                            class: "notification",
                            "No objects found."
                        }
                    } else {
                        ObjectsTable {
                            page,
                            now: now,
                            on_view: move |item| {
                                tx.send(Msg::ViewObject(item));
                            },
                            on_download: move |item| {
                                tx.send(Msg::Download(item));
                            },
                            on_delete: move |item| {
                                tx.send(Msg::DeleteObject(item));
                            },
                        }
                    }
                }
            }
            LoadState::Loaded(Err(err)) => {
                rsx! {
                    div {
                        class: "notification is-danger",
                        "Error loading objects: {err}"
                    }
                }
            }
        }
    };

    rsx! {
        div {
            h1 {
                class: "title is-3",
                "{store.config.config.name}"
            }

            {modal}

            div {
                {breadcrumbs}
            }

            {action_bar}

            if show_settings() {
                div { class: "box",
                    BrowserSettingsForm {
                        initial_pagination: manual_pagination(),
                        initial_pagination_size: pagination_size(),
                        on_change_pagination: move |val| {
                            manual_pagination.set(val);
                        },
                        on_change_pagination_size: move |val| {
                            pagination_size.set(val);
                        },
                        on_close: move |_| {
                            show_settings.set(false);
                        },
                    }
                }
            }

            div {
                div {
                    class: "box",
                    {contents}
                }
                // Manual pagination: Load more button
                if manual_pagination() && next_cursor().is_some() {
                    button {
                        class: "button is-fullwidth is-link",
                        onclick: move |_| tx.send(Msg::LoadMore),
                        "Load more"
                    }
                }
            }
        }
    }
}
