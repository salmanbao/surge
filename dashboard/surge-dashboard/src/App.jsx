import { useState, useEffect } from "react";
import { Routes, Route } from "react-router-dom";
import { Toaster } from "react-hot-toast";
import { Box, Paper, Typography, Button, Grid, Chip } from "@mui/material";
import {
  Refresh as RefreshIcon,
  AccessTime as AccessTimeIcon,
  Pause as PauseIcon,
} from "@mui/icons-material";
import { Layout } from "./components/Layout";
import { QueueTableContent } from "./components/QueueTableContent";
import { QueueDetails } from "./components/QueueDetails";
import { HandlersView } from "./components/HandlersView";
import { CombinedQueueChart } from "./components/Charts";
import { api } from "./services/api";
import {
  sortNamespaces,
  selectInitialNamespace,
  selectFallbackNamespace,
  saveNamespace,
} from "./utils/namespaceHelpers";
import { handleApiError } from "./utils/errorHelpers";
import { buildStatsMap } from "./utils/statsHelpers";
import "./App.css";

function HomePage({
  namespaces,
  selectedNs,
  onNamespaceChange,
  onNamespacesChange,
}) {
  const [queues, setQueues] = useState([]);
  const [queueStats, setQueueStats] = useState({});
  const [loading, setLoading] = useState(true);
  const [lastUpdate, setLastUpdate] = useState(null);
  const [initialLoad, setInitialLoad] = useState(true);
  const [queuePage, setQueuePage] = useState(1);
  const [queuePageSize, setQueuePageSize] = useState(25);

  const fetchQueues = async () => {
    try {
      const data = await api.getQueues();
      setQueues(Array.isArray(data) ? data : []);
      setLoading(false);
      setLastUpdate(new Date());
    } catch (err) {
      handleApiError(err, "Failed to fetch queues");
      setLoading(false);
    }
  };

  const fetchNamespaces = async () => {
    try {
      const data = await api.getNamespaces();
      const safeData = Array.isArray(data) ? data : [];
      const sorted = sortNamespaces(safeData);
      onNamespacesChange(sorted);

      if (sorted.length === 0) return;

      if (initialLoad) {
        selectInitialNamespace(sorted, onNamespaceChange);
        setInitialLoad(false);
        return;
      }

      selectFallbackNamespace(sorted, selectedNs, onNamespaceChange);
    } catch (err) {
      handleApiError(err, "Failed to fetch namespaces");
    }
  };

  const fetchValues = () => {
    fetchQueues();
    fetchNamespaces();
  };

  const fetchBatchStats = async () => {
    try {
      const data = await api.getBatchQueueStats(selectedNs);
      setQueueStats(buildStatsMap(data));
    } catch (err) {
      handleApiError(err, "Batch stats error");
    }
  };

  useEffect(() => {
    fetchValues();
    const interval = setInterval(fetchValues, 10000);
    return () => clearInterval(interval);
  }, []);

  useEffect(() => {
    if (namespaces.length > 0) {
      fetchBatchStats();
      const interval = setInterval(fetchBatchStats, 3000);
      return () => clearInterval(interval);
    }
  }, [selectedNs, namespaces]);

  const filteredQueues = queues.filter((q) => q.namespace === selectedNs);

  const queuesWithStats = filteredQueues.map((q) => ({
    ...q,
    stats: queueStats[`${q.namespace}:${q.name}`],
  }));

  const totalQueuePages = Math.ceil(queuesWithStats.length / queuePageSize);
  const startQueueIndex = (queuePage - 1) * queuePageSize;
  const endQueueIndex = startQueueIndex + queuePageSize;
  const paginatedQueues = queuesWithStats.slice(startQueueIndex, endQueueIndex);

  const handleQueuePageChange = (event, page) => {
    setQueuePage(page);
    window.scrollTo({ top: 0, behavior: "smooth" });
  };

  const handleQueuePageSizeChange = (newSize) => {
    setQueuePageSize(newSize);
    setQueuePage(1);
  };

  useEffect(() => {
    setQueuePage(1);
  }, [selectedNs]);

  const handleStatsUpdate = (namespace, queueName, stats) => {
    setQueueStats((prev) => ({
      ...prev,
      [`${namespace}:${queueName}`]: stats,
    }));
  };

  const aggregateStats = queuesWithStats.reduce(
    (acc, q) => {
      const stats = q.stats || {};
      if (stats.paused) acc.pausedQueues += 1;
      return acc;
    },
    {
      pausedQueues: 0,
    },
  );

  return (
    <Box sx={{ maxWidth: "1400px", mx: "auto", p: 3 }}>
      {/* Header Section */}
      <Box sx={{ mb: 4 }}>
        <Box
          sx={{
            display: "flex",
            alignItems: "center",
            justifyContent: "space-between",
            mb: 3,
          }}
        >
          <Box>
            <Typography variant="body2" color="text.secondary">
              Real-time monitoring and management for your job queues
            </Typography>
          </Box>
          <Box sx={{ display: "flex", alignItems: "center", gap: 2 }}>
            {lastUpdate && (
              <Box
                sx={{
                  display: "flex",
                  alignItems: "center",
                  gap: 1,
                  color: "text.secondary",
                }}
              >
                <AccessTimeIcon sx={{ fontSize: 18 }} />
                <Typography variant="caption">
                  Updated {lastUpdate.toLocaleTimeString()}
                </Typography>
              </Box>
            )}
            <Button
              startIcon={<RefreshIcon />}
              onClick={fetchValues}
              variant="outlined"
              size="small"
            >
              Refresh
            </Button>
          </Box>
        </Box>
      </Box>

      {/* Charts Section */}
      {filteredQueues.length > 0 && (
        <Grid container spacing={3} sx={{ mb: 3 }}>
          <Grid item xs={12}>
            <Paper elevation={2} sx={{ p: 3 }}>
              <Box
                sx={{ display: "flex", justifyContent: "space-between", mb: 2 }}
              >
                <Typography variant="h6" sx={{ fontWeight: 600 }}>
                  Queue Overview
                </Typography>
              </Box>
              <CombinedQueueChart queues={queuesWithStats} />
            </Paper>
          </Grid>
        </Grid>
      )}

      {/* Queue Table Section */}
      <Paper elevation={2}>
        <Box sx={{ p: 3, borderBottom: 1, borderColor: "divider" }}>
          <Box
            sx={{
              display: "flex",
              justifyContent: "space-between",
              alignItems: "center",
            }}
          >
            <Box>
              <Typography variant="h6" sx={{ fontWeight: 600 }}>
                Queues
              </Typography>
              <Typography
                variant="body2"
                color="text.secondary"
                sx={{ mt: 0.5 }}
              >
                Click on any queue to view detailed statistics
              </Typography>
            </Box>
            {aggregateStats.pausedQueues > 0 && (
              <Chip
                icon={<PauseIcon />}
                label={`${aggregateStats.pausedQueues} paused`}
                color="warning"
                variant="outlined"
              />
            )}
          </Box>
        </Box>

        <Box sx={{ p: 3 }}>
          <QueueTableContent
            loading={loading}
            filteredQueues={filteredQueues}
            paginatedQueues={paginatedQueues}
            queuesWithStats={queuesWithStats}
            selectedNs={selectedNs}
            namespaces={namespaces}
            onStatsUpdate={handleStatsUpdate}
            queuePage={queuePage}
            queuePageSize={queuePageSize}
            totalQueuePages={totalQueuePages}
            onPageChange={handleQueuePageChange}
            onPageSizeChange={handleQueuePageSizeChange}
          />
        </Box>
      </Paper>
    </Box>
  );
}

function HomePageWrapper({
  namespaces,
  selectedNs,
  onNamespaceChange,
  onNamespacesChange,
}) {
  return (
    <Layout
      namespaces={namespaces}
      selectedNs={selectedNs}
      onNamespaceChange={onNamespaceChange}
    >
      <HomePage
        namespaces={namespaces}
        selectedNs={selectedNs}
        onNamespaceChange={onNamespaceChange}
        onNamespacesChange={onNamespacesChange}
      />
    </Layout>
  );
}

import { getStoredNamespace } from "./utils/namespaceHelpers";

function App() {
  const [namespaces, setNamespaces] = useState([]);
  const [selectedNs, setSelectedNs] = useState(getStoredNamespace);

  const handleNamespaceChange = (newNs) => {
    setSelectedNs(newNs);
    saveNamespace(newNs);
  };

  return (
    <>
      <Toaster position="top-right" />
      <Routes>
        <Route
          path="/"
          element={
            <HomePageWrapper
              namespaces={namespaces}
              selectedNs={selectedNs}
              onNamespaceChange={handleNamespaceChange}
              onNamespacesChange={setNamespaces}
            />
          }
        />
        <Route path="/queue/:namespace/:queue" element={<QueueDetails />} />
        <Route path="/handlers" element={<HandlersView />} />
      </Routes>
    </>
  );
}

export default App;
