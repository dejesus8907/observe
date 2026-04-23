import React from "react";
import ReactDOM from "react-dom/client";
import { BrowserRouter, Routes, Route, Navigate } from "react-router-dom";
import { QueryProvider } from "./context/QueryContext";
import { AppShell } from "./components/AppShell";
import OverviewPage from "./pages/OverviewPage";
import IncidentsPage from "./pages/IncidentsPage";
import TopologyPage from "./pages/TopologyPage";
import AlertsSLOsPage from "./pages/AlertsSLOsPage";
import ServicesPage from "./pages/ServicesPage";
import TelemetryPage from "./pages/TelemetryPage";
import TruthOperationsPage from "./pages/TruthOperationsPage";
import RCAPage from "./pages/RCAPage";

ReactDOM.createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <QueryProvider>
      <BrowserRouter>
        <Routes>
          <Route element={<AppShell />}>
            <Route index element={<OverviewPage />} />
            <Route path="/incidents" element={<IncidentsPage />} />
            <Route path="/topology" element={<TopologyPage />} />
            <Route path="/alerts" element={<AlertsSLOsPage />} />
            <Route path="/services" element={<ServicesPage />} />
            <Route path="/telemetry" element={<TelemetryPage />} />
            <Route path="/truth" element={<TruthOperationsPage />} />
            <Route path="/rca" element={<RCAPage />} />
            <Route path="*" element={<Navigate to="/" replace />} />
          </Route>
        </Routes>
      </BrowserRouter>
    </QueryProvider>
  </React.StrictMode>
);
