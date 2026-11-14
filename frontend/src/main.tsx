import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import "./styles/tokens.css";
import "./styles/app.css";
import { App } from "./App";

// Apply the saved theme before first paint to avoid a flash.
try {
  const t = localStorage.getItem("quorumdb.theme");
  if (t === "dark" || t === "light") document.documentElement.setAttribute("data-theme", t);
} catch {
  /* ignore */
}

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <App />
  </StrictMode>,
);
