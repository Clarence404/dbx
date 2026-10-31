//! Lowers the main WebView2's memory target while the window is not on screen.
//!
//! The app can sit in the tray or minimized for hours. WebView2 documents that setting
//! `MemoryUsageTargetLevel = Low` on an inactive WebView lets the engine drop cached data and
//! page memory to disk, and restoring `Normal` on return keeps full performance. See
//! <https://learn.microsoft.com/en-us/microsoft-edge/webview2/concepts/performance#use-memory-management-apis>.
//!
//! Only a window the user cannot see qualifies. Merely losing focus (alt-tab to documentation)
//! keeps `Normal`: trimming a page the user is about to look at again makes the return sluggish.
//!
//! WKWebView and WebKitGTK expose no equivalent, so everything except the pure decision is
//! Windows-only.

/// A window that is minimized or hidden to the tray cannot be seen.
#[cfg(any(target_os = "windows", test))]
fn wants_low_memory(minimized: bool, visible: bool) -> bool {
    minimized || !visible
}

#[cfg(target_os = "windows")]
mod windows_impl {
    use super::wants_low_memory;
    use std::sync::atomic::{AtomicBool, Ordering};
    use tauri::{Manager, Runtime};
    use webview2_com::Microsoft::Web::WebView2::Win32::{ICoreWebView2_19, COREWEBVIEW2_MEMORY_USAGE_TARGET_LEVEL};
    use windows_core::Interface;

    const MAIN_WINDOW_LABEL: &str = "main";
    // COREWEBVIEW2_MEMORY_USAGE_TARGET_LEVEL_NORMAL / _LOW
    const LEVEL_NORMAL: i32 = 0;
    const LEVEL_LOW: i32 = 1;

    static LOW_APPLIED: AtomicBool = AtomicBool::new(false);

    fn apply(webview: &tauri::webview::PlatformWebview, low: bool) -> Result<(), String> {
        // SAFETY: `with_webview` runs this closure on the WebView2 UI thread, which the COM
        // contract requires. `ICoreWebView2_19` needs runtime >= 114; older runtimes fail the
        // cast and the caller only logs, leaving the default memory behavior unchanged.
        unsafe {
            let core = webview.controller().CoreWebView2().map_err(|error| error.to_string())?;
            let core19 = core.cast::<ICoreWebView2_19>().map_err(|error| error.to_string())?;
            let level = COREWEBVIEW2_MEMORY_USAGE_TARGET_LEVEL(if low { LEVEL_LOW } else { LEVEL_NORMAL });
            core19.SetMemoryUsageTargetLevel(level).map_err(|error| error.to_string())
        }
    }

    /// Re-evaluates the target level from the main window's current visibility.
    /// Cheap and idempotent: the WebView is only touched when the desired level changes.
    pub fn sync<R: Runtime>(app: &tauri::AppHandle<R>) {
        let Some(window) = app.get_webview_window(MAIN_WINDOW_LABEL) else {
            return;
        };
        let minimized = window.is_minimized().unwrap_or(false);
        let visible = window.is_visible().unwrap_or(true);
        let low = wants_low_memory(minimized, visible);
        if LOW_APPLIED.swap(low, Ordering::SeqCst) == low {
            return;
        }
        let result = window.with_webview(move |webview| {
            if let Err(error) = apply(&webview, low) {
                eprintln!("[WARN] failed to set WebView2 memory usage target: {error}");
            }
        });
        if let Err(error) = result {
            LOW_APPLIED.store(!low, Ordering::SeqCst);
            eprintln!("[WARN] failed to dispatch WebView2 memory usage target: {error}");
        }
    }
}

/// Called after anything that can change whether the main window is on screen.
#[cfg(target_os = "windows")]
pub fn sync<R: tauri::Runtime>(app: &tauri::AppHandle<R>) {
    windows_impl::sync(app);
}

#[cfg(not(target_os = "windows"))]
pub fn sync<R: tauri::Runtime>(_app: &tauri::AppHandle<R>) {}

#[cfg(test)]
mod tests {
    use super::wants_low_memory;

    #[test]
    fn only_an_invisible_window_gets_the_low_target() {
        assert!(!wants_low_memory(false, true));
        assert!(wants_low_memory(true, true));
        assert!(wants_low_memory(false, false));
        assert!(wants_low_memory(true, false));
    }
}
