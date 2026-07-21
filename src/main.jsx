import React from "react";
import { createRoot } from "react-dom/client";
import { App } from "./App.jsx";

class ErrorBoundary extends React.Component {
  constructor(props) {
    super(props);
    this.state = { failed: false };
  }

  static getDerivedStateFromError() {
    return { failed: true };
  }

  render() {
    if (!this.state.failed) return this.props.children;
    return (
      <div className="auth-gate">
        <div style={{ textAlign: "center", maxWidth: 380 }}>
          <h1 style={{ fontFamily: "var(--serif)", letterSpacing: ".14em", textTransform: "uppercase", fontWeight: 560 }}>Mira</h1>
          <p style={{ color: "var(--muted)", fontSize: 14, lineHeight: 1.6 }}>Something went wrong on this screen. Your closet is safe.</p>
          <button
            type="button"
            style={{ marginTop: 16, border: "1px solid var(--ink)", background: "var(--ink)", color: "var(--paper)", padding: "12px 24px", letterSpacing: ".14em", textTransform: "uppercase", fontSize: 12, cursor: "pointer" }}
            onClick={() => window.location.reload()}
          >
            Reload
          </button>
        </div>
      </div>
    );
  }
}
import "@fontsource-variable/cormorant";
import "./styles.css";

createRoot(document.getElementById("root")).render(
  <React.StrictMode>
    <ErrorBoundary>
      <App />
    </ErrorBoundary>
  </React.StrictMode>,
);

if ("serviceWorker" in navigator && import.meta.env.PROD) {
  window.addEventListener("load", () => navigator.serviceWorker.register("/sw.js"));
}
