mod table;

use std::sync::Arc;

use dioxus::{core::Task, prelude::*};
use futures::StreamExt as _;
use objstore::{ListArgs, ObjectMeta};

use crate::{
    cmp::{
        object_delete_modal::ObjectDeleteModal,
        util::loader::{LoadState, Spinner},
    },
    context::ActiveStore,
};

use table::ObjectsTable;

#[derive(Clone, Debug, PartialEq, Eq)]
enum ModalView {
    DeleteObject { meta: Arc<ObjectMeta> },
}

enum Msg {
    GotoPath(String),
    Download(Arc<ObjectMeta>),
    DeleteObject(Arc<ObjectMeta>),
    ObjectDeleted { key: String },
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

    let tx = use_coroutine::<Msg, _, _>({
        let store = store.store.clone();
        move |mut rx| {
            let store = store.clone();
            async move {
                let mut task: Option<Task> = None;

                let mut load = |args: ListArgs| {
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
                        tracing::info!("Page loaded, result: {res:?}");
                        match res {
                            Ok(objects_page) => {
                                tracing::info!(
                                    "Loaded page, items: {}, prefixes: {}",
                                    objects_page.items.len(),
                                    objects_page.prefixes.as_ref().map_or(0, |p| p.len())
                                );
                                let new_page = Page {
                                    objects: objects_page
                                        .items
                                        .into_iter()
                                        .map(|meta| Arc::new(meta))
                                        .collect(),
                                    prefixes: objects_page.prefixes,
                                    next_cursor: objects_page.next_cursor.clone(),
                                };

                                next_cursor.set(objects_page.next_cursor.clone());
                                page.set(new_page);
                                load_state.set(LoadState::Loaded(Ok(())));
                            }
                            Err(err) => {
                                tracing::error!("Error loading page: {err}");
                                load_state.set(LoadState::Loaded(Err(err.to_string())));
                            }
                        };
                    });
                    task = Some(f);
                };

                // TODO: respect URL path!
                load(ListArgs::new());

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
                            let args = ListArgs::new().with_prefix(path);
                            load(args)
                        }
                        Msg::Download(meta) => {
                            tracing::info!("starting object download: {}", &meta.key);
                            let size = meta.size;
                            let on_progress: crate::store::DownloadProgressCallback =
                                Box::new(move |size: u64| {});
                            match crate::store::download_object(&store, &meta, None, on_progress)
                                .await
                            {
                                Ok(()) => {
                                    tracing::info!(
                                        "Object download started successfully: {}",
                                        meta.key
                                    );
                                }
                                Err(err) => {
                                    tracing::error!("Failed to start object download: {}", err);
                                    // TODO: show error toas!
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
                            "No items found."
                        }
                    } else {
                        ObjectsTable {
                            page,
                            now: now,
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

            div {
                div {
                    class: "box",
                    {contents}
                }
            }
        }
    }
}
