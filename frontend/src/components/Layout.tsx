import { NavLink, Outlet, useLocation } from "react-router-dom";
import { Activity, FlaskConical, Gauge, LayoutDashboard, ListTree, Moon, Network, Settings, Sun, Timer } from "lucide-react";
import { api } from "../api/client";
import type { ClusterSummary } from "../api/types";
import { usePoll } from "../hooks/usePoll";
import { useTheme } from "../hooks/useTheme";
import { HealthBadge, Badge } from "./ui";

const NAV = [
  { section: "Cluster" },
  { to: "/", label: "Overview", icon: LayoutDashboard, end: true },
  { to: "/cluster", label: "Cluster Explorer", icon: Network },
  { to: "/operations", label: "Operations", icon: Activity },
  { section: "FaultLab" },
  { to: "/faultlab", label: "FaultLab", icon: FlaskConical, tag: "LAB" },
  { to: "/timeline", label: "Event Timeline", icon: ListTree },
  { section: "Measure" },
  { to: "/benchmarks", label: "Benchmarks", icon: Gauge },
  { to: "/settings", label: "Settings", icon: Settings },
] as const;

const TITLES: Record<string, string> = {
  "/": "Overview", "/cluster": "Cluster Explorer", "/operations": "Operations", "/faultlab": "FaultLab",
  "/timeline": "Event Timeline", "/benchmarks": "Benchmarks", "/settings": "Settings",
};

export function Layout() {
  const location = useLocation();
  const [theme, setTheme] = useTheme();
  const cluster = usePoll((signal) => api<ClusterSummary>("/cluster", { signal }), [], 2000);
  const title = TITLES["/" + location.pathname.split("/")[1]] ?? "QuorumDB";
  const c = cluster.data;
  const dark = theme === "dark" || (theme === "system" && window.matchMedia("(prefers-color-scheme: dark)").matches);

  return (
    <div className="shell">
      <aside className="sidebar">
        <div className="brand">
          <img className="brand-mark" src="/favicon.svg" width={26} height={26} alt="" />
          <div>QuorumDB<small>Raft key-value store</small></div>
        </div>
        {NAV.map((item, i) =>
          "section" in item ? (
            <div className="nav-section" key={i}>{item.section}</div>
          ) : (
            <NavLink key={item.to} to={item.to} end={"end" in item ? item.end : false}
              className={({ isActive }) => `nav-link ${isActive ? "active" : ""}`}>
              <item.icon size={16} strokeWidth={1.9} />
              {item.label}
              {"tag" in item && <span className="tag">{item.tag}</span>}
            </NavLink>
          ),
        )}
        <div className="sidebar-foot">
          {c ? <>Cluster <span className="mono">{c.clusterId}</span> · {c.size} nodes</> : "Connecting…"}
          <div>Educational project. Not for production data.</div>
        </div>
      </aside>
      <div className="main">
        <header className="topbar">
          <span className="title">{title}</span>
          <span className="spacer" />
          {cluster.error && !c && <Badge tone="critical">API unreachable</Badge>}
          {c && (
            <div className="row small secondary">
              <HealthBadge health={c.health} />
              <span className="row" style={{ gap: 4 }}><Timer size={14} />term <b className="num">{c.term}</b></span>
              <span>leader <b className="mono">{c.leaderId ?? "none"}</b></span>
            </div>
          )}
          <button className="btn icon ghost" aria-label={dark ? "Switch to light theme" : "Switch to dark theme"}
            title={dark ? "Light theme" : "Dark theme"} onClick={() => setTheme(dark ? "light" : "dark")}>
            {dark ? <Sun /> : <Moon />}
          </button>
        </header>
        <main className="content">
          <Outlet />
        </main>
      </div>
    </div>
  );
}
