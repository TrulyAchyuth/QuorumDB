import { BrowserRouter, Route, Routes } from "react-router-dom";
import { Layout } from "./components/Layout";
import { Overview } from "./pages/Overview";
import { ClusterExplorer } from "./pages/ClusterExplorer";
import { Operations } from "./pages/Operations";
import { FaultLab } from "./pages/faultlab/FaultLab";
import { RunView } from "./pages/faultlab/RunView";
import { Compare } from "./pages/faultlab/Compare";
import { Timeline } from "./pages/Timeline";
import { Benchmarks } from "./pages/Benchmarks";
import { Settings } from "./pages/Settings";
import { Empty } from "./components/ui";

export function App() {
  return (
    <BrowserRouter>
      <Routes>
        <Route element={<Layout />}>
          <Route index element={<Overview />} />
          <Route path="cluster" element={<ClusterExplorer />} />
          <Route path="operations" element={<Operations />} />
          <Route path="faultlab" element={<FaultLab />} />
          <Route path="faultlab/runs/:id" element={<RunView />} />
          <Route path="faultlab/compare" element={<Compare />} />
          <Route path="timeline" element={<Timeline />} />
          <Route path="benchmarks" element={<Benchmarks />} />
          <Route path="settings" element={<Settings />} />
          <Route path="*" element={<Empty title="Page not found">That page does not exist.</Empty>} />
        </Route>
      </Routes>
    </BrowserRouter>
  );
}
