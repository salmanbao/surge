import React from "react";
import { useNavigate } from "react-router-dom";

import {
  Box,
  Paper,
  Typography,
  Button,
  Grid,
  Chip,
  IconButton,
  Stack,
  Alert,
  CircularProgress,
  Pagination,
  Tabs,
  Tab,
  Card,
  CardContent,
  Skeleton,
  FormControl,
  Select,
  MenuItem,
} from "@mui/material";
import {
  ArrowBack as ArrowBackIcon,
  Pause as PauseIcon,
  PlayArrow as PlayArrowIcon,
  Refresh as RefreshIcon,
  AccessTime as AccessTimeIcon,
  Folder as FolderIcon,
  CheckCircle as CheckCircleIcon,
  Error as ErrorIcon,
  Schedule as ScheduleIcon,
  Sync as SyncIcon,
} from "@mui/icons-material";
import { useState, useEffect, useCallback } from "react";
import { Layout } from "./Layout";
import { ConfirmationModal } from "./ConfirmationModal";
import { ScheduledJobsTable } from "./tables/ScheduledJobsTable";
import { SkeletonLoader } from "./SkeletonLoader";
import { DLQContent } from "./DLQContent";
import {
  QueueDetailsProvider,
  useQueueDetails,
} from "../context/QueueDetailsContext";
import { getModalConfig, handleModalConfirm } from "../utils/modalConfig";
import {
  calculateErrorRate,
  getErrorRateSeverity,
} from "../utils/errorRateHelpers";
import { getPauseResumeButtonProps } from "../utils/buttonHelpers";
import { api } from "../services/api";
import {
  getStoredNamespace,
  saveNamespace,
  sortNamespaces,
} from "../utils/namespaceHelpers";

export function QueueDetails() {
  const navigate = useNavigate();
  const [namespaces, setNamespaces] = useState([]);
  const [selectedNs, setSelectedNs] = useState(getStoredNamespace());

  useEffect(() => {
    const fetchNamespaces = async () => {
      try {
        const data = await api.getNamespaces();
        const safeData = Array.isArray(data) ? data : [];
        const sorted = sortNamespaces(safeData);
        setNamespaces(sorted);
        if (sorted.length > 0 && !sorted.includes(selectedNs)) {
          const newNs = sorted[0];
          setSelectedNs(newNs);
          saveNamespace(newNs);
        }
      } catch (err) {
        console.error("Failed to fetch namespaces:", err);
      }
    };
    fetchNamespaces();
    const interval = setInterval(fetchNamespaces, 30000);
    return () => clearInterval(interval);
  }, [selectedNs]);

  const handleNamespaceChange = useCallback(
    (newNs) => {
      setSelectedNs(newNs);
      saveNamespace(newNs);
      navigate("/");
    },
    [navigate],
  );

  return (
    <QueueDetailsProvider>
      <QueueDetailsContent
        namespaces={namespaces}
        selectedNs={selectedNs}
        onNamespaceChange={handleNamespaceChange}
      />
    </QueueDetailsProvider>
  );
}

function QueueDetailsContent({ namespaces, selectedNs, onNamespaceChange }) {
  const navigate = useNavigate();
  const {
    namespace,
    queue,
    stats,
    loading,
    lastUpdate,
    activeTab,
    handleTabChange,
    confirmModal,
    setConfirmModal,

    // DLQ
    dlqJobs,
    dlqLoading,
    dlqPage,
    setDlqPage,
    dlqPageSize,
    setDlqPageSize,
    dlqTotal,
    fetchDLQ,
    searchQuery,
    setSearchQuery,
    viewMode,
    setViewMode,
    selectedJobs,
    toggleJobSelection,
    selectAll,

    // Scheduled
    scheduledJobs,
    scheduledLoading,
    scheduledPage,
    setScheduledPage,
    scheduledPageSize,
    setScheduledPageSize,
    scheduledTotal,
    fetchScheduled,

    // Actions
    actionLoading,
    bulkActionLoading,
    handleBulkRetry,
    handlePauseResume,
  } = useQueueDetails();

  const handleBulkRetryClick = () => {
    if (selectedJobs.size === 0) return;
    setConfirmModal({
      isOpen: true,
      type: "bulkRetry",
      data: { count: selectedJobs.size },
    });
  };

  const filteredJobs = dlqJobs.filter((job) => {
    if (!searchQuery) return true;
    const query = searchQuery.toLowerCase();
    const jobId = (job.id || "").toLowerCase();
    const error = (job.last_error || "").toLowerCase();
    const topic = (job.topic || "").toLowerCase();
    return (
      jobId.includes(query) || error.includes(query) || topic.includes(query)
    );
  });

  const handlePageChange = (event, page) => {
    setDlqPage(page);
    window.scrollTo({ top: 0, behavior: "smooth" });
  };

  const handlePageSizeChange = (newSize) => {
    setDlqPageSize(newSize);
    setDlqPage(1); // Reset to page 1
  };

  const handlePauseResumeClick = () => {
    if (!stats) return;
    setConfirmModal({
      isOpen: true,
      type: stats.paused ? "resume" : "pause",
      data: null,
    });
  };

  if (loading) {
    return (
      <Layout
        namespaces={namespaces}
        selectedNs={selectedNs}
        onNamespaceChange={onNamespaceChange}
      >
        <Box sx={{ maxWidth: "1400px", mx: "auto", p: 3 }}>
          <SkeletonLoader />
        </Box>
      </Layout>
    );
  }

  if (!stats) {
    return (
      <Layout
        namespaces={namespaces}
        selectedNs={selectedNs}
        onNamespaceChange={onNamespaceChange}
      >
        <Box sx={{ maxWidth: "1400px", mx: "auto", p: 3 }}>
          <Alert severity="error" icon={<ErrorIcon />}>
            <Typography variant="h6" sx={{ mb: 1 }}>
              Failed to load queue details
            </Typography>
            <Typography variant="body2">
              Please check your connection and try again.
            </Typography>
          </Alert>
        </Box>
      </Layout>
    );
  }

  const total = (stats.processed || 0) + (stats.failed || 0);
  const errorRate = calculateErrorRate(stats.processed || 0, stats.failed || 0);
  const errorRateSeverity = getErrorRateSeverity(errorRate);
  const isPaused = stats.paused === true;

  return (
    <Layout
      namespaces={namespaces}
      selectedNs={selectedNs}
      onNamespaceChange={onNamespaceChange}
    >
      <Box sx={{ maxWidth: "1400px", mx: "auto", p: 3 }}>
        {/* Header */}
        <Box sx={{ mb: 4 }}>
          <Button
            startIcon={<ArrowBackIcon />}
            onClick={() => navigate("/")}
            sx={{ mb: 3 }}
            color="inherit"
          >
            Back to Queues
          </Button>

          <Box
            sx={{
              display: "flex",
              justifyContent: "space-between",
              alignItems: "flex-start",
            }}
          >
            <Box>
              <Box
                sx={{ display: "flex", alignItems: "center", gap: 2, mb: 1 }}
              >
                <Typography
                  variant="h4"
                  component="h1"
                  sx={{ fontWeight: 600 }}
                >
                  {queue}
                </Typography>
                <StatusBadge isPaused={isPaused} />
              </Box>
              <Box
                sx={{
                  display: "flex",
                  alignItems: "center",
                  gap: 2,
                  color: "text.secondary",
                }}
              >
                <Box sx={{ display: "flex", alignItems: "center", gap: 0.5 }}>
                  <FolderIcon sx={{ fontSize: 18 }} />
                  <Typography variant="body2">{namespace}</Typography>
                </Box>
                {lastUpdate && (
                  <Box sx={{ display: "flex", alignItems: "center", gap: 0.5 }}>
                    <AccessTimeIcon sx={{ fontSize: 18 }} />
                    <Typography variant="body2">
                      Updated {lastUpdate.toLocaleTimeString()}
                    </Typography>
                  </Box>
                )}
              </Box>
            </Box>

            <PauseResumeButton
              isPaused={isPaused}
              actionLoading={actionLoading}
              onClick={handlePauseResumeClick}
            />
          </Box>
        </Box>

        {/* Metrics Grid */}
        <Grid container spacing={3} sx={{ mb: 3 }}>
          <Grid item xs={12} sm={6} md={3}>
            <MetricCard
              title="Pending"
              value={stats.pending}
              color="default"
              icon={<ScheduleIcon />}
            />
          </Grid>
          <Grid item xs={12} sm={6} md={3}>
            <MetricCard
              title="Processing"
              value={stats.processing}
              color="primary"
              icon={<SyncIcon />}
            />
          </Grid>
          <Grid item xs={12} sm={6} md={3}>
            <MetricCard
              title="Processed"
              value={stats.processed || 0}
              color="success"
              icon={<CheckCircleIcon />}
            />
          </Grid>
          <Grid item xs={12} sm={6} md={3}>
            <MetricCard
              title="Failed"
              value={stats.failed}
              color="error"
              icon={<ErrorIcon />}
            />
          </Grid>
        </Grid>

        {/* Error Rate & Summary */}
        <Grid container spacing={3} sx={{ mb: 3 }}>
          <Grid item xs={12} lg={6}>
            <Paper
              elevation={2}
              sx={{
                height: "100%",
                p: 3,
                background:
                  "linear-gradient(to bottom right, #ffffff, #f5f5f5)",
              }}
            >
              <Box
                sx={{
                  display: "flex",
                  justifyContent: "space-between",
                  alignItems: "center",
                  mb: 2,
                }}
              >
                <Typography variant="h6" sx={{ fontWeight: 600 }}>
                  Error Rate
                </Typography>
                <Chip
                  label={errorRateSeverity.label}
                  color={errorRateSeverity.color}
                  size="small"
                />
              </Box>
              <Box
                sx={{ display: "flex", alignItems: "baseline", gap: 1, mb: 2 }}
              >
                <Typography variant="h3" sx={{ fontWeight: 700 }}>
                  {errorRate}%
                </Typography>
                <Typography variant="body2" color="text.secondary">
                  error rate
                </Typography>
              </Box>
              <Typography variant="body2" color="text.secondary">
                <strong>{stats.failed}</strong> failed out of{" "}
                <strong>{total}</strong> total tasks
              </Typography>
            </Paper>
          </Grid>

          <Grid item xs={12} lg={6}>
            <Paper
              elevation={2}
              sx={{
                height: "100%",
                p: 3,
                background:
                  "linear-gradient(to bottom right, #ffffff, #e3f2fd)",
              }}
            >
              <Typography variant="h6" sx={{ fontWeight: 600, mb: 2 }}>
                Queue Summary
              </Typography>
              <Stack spacing={2}>
                <Box sx={{ display: "flex", justifyContent: "space-between" }}>
                  <Typography variant="body2" color="text.secondary">
                    Total Jobs
                  </Typography>
                  <Typography variant="h6" sx={{ fontWeight: 600 }}>
                    {stats.pending +
                      stats.processing +
                      (stats.processed || 0) +
                      stats.failed}
                  </Typography>
                </Box>
                <Box sx={{ display: "flex", justifyContent: "space-between" }}>
                  <Typography variant="body2" color="text.secondary">
                    Success Rate
                  </Typography>
                  <Typography
                    variant="h6"
                    sx={{ fontWeight: 600, color: "success.main" }}
                  >
                    {total > 0
                      ? ((stats.processed / total) * 100).toFixed(1)
                      : "0.0"}
                    %
                  </Typography>
                </Box>
                <Box sx={{ display: "flex", justifyContent: "space-between" }}>
                  <Typography variant="body2" color="text.secondary">
                    Active Workers
                  </Typography>
                  <Typography
                    variant="h6"
                    sx={{ fontWeight: 600, color: "primary.main" }}
                  >
                    {stats.processing}
                  </Typography>
                </Box>
              </Stack>
            </Paper>
          </Grid>
        </Grid>

        {/* Tabs for Job Lists */}
        <Paper elevation={2} sx={{ p: 3 }}>
          <Box sx={{ borderBottom: 1, borderColor: "divider", mb: 3 }}>
            <Tabs value={activeTab} onChange={handleTabChange}>
              <Tab
                label={
                  <Box sx={{ display: "flex", alignItems: "center", gap: 1 }}>
                    <ErrorIcon color={activeTab === 0 ? "error" : "disabled"} />
                    Failed (DLQ)
                    <Chip
                      label={stats.failed || 0}
                      size="small"
                      color={stats.failed > 0 ? "error" : "default"}
                      sx={{ ml: 1, height: 20 }}
                    />
                  </Box>
                }
              />
              <Tab
                label={
                  <Box sx={{ display: "flex", alignItems: "center", gap: 1 }}>
                    <ScheduleIcon
                      color={activeTab === 1 ? "primary" : "disabled"}
                    />
                    Scheduled
                    <Chip
                      label={scheduledTotal}
                      size="small"
                      color="primary"
                      variant="outlined"
                      sx={{ ml: 1, height: 20 }}
                    />
                  </Box>
                }
              />
            </Tabs>
          </Box>

          {activeTab === 0 && (
            <Box>
              <Box
                sx={{
                  display: "flex",
                  justifyContent: "space-between",
                  alignItems: "center",
                  mb: 3,
                }}
              >
                <Box>
                  <Typography variant="h6" sx={{ fontWeight: 600, mb: 0.5 }}>
                    Failed Jobs
                  </Typography>
                  <Typography variant="body2" color="text.secondary">
                    Jobs that exceeded maximum retries
                  </Typography>
                </Box>
                <IconButton
                  onClick={() => fetchDLQ()}
                  disabled={dlqLoading}
                  color="primary"
                >
                  {dlqLoading ? (
                    <CircularProgress size={20} />
                  ) : (
                    <RefreshIcon />
                  )}
                </IconButton>
              </Box>

              <DLQContent
                loading={dlqLoading}
                jobs={dlqJobs}
                filteredJobs={filteredJobs}
                searchQuery={searchQuery}
                onSearchChange={setSearchQuery}
                viewMode={viewMode}
                onViewModeChange={setViewMode}
                selectedJobs={selectedJobs}
                onToggleSelection={toggleJobSelection}
                onSelectAll={() => selectAll(filteredJobs)}
                onRetry={fetchDLQ}
                onBulkRetryClick={handleBulkRetryClick}
                bulkActionLoading={bulkActionLoading}
                page={dlqPage}
                pageSize={dlqPageSize}
                total={dlqTotal}
                onPageChange={handlePageChange}
                onPageSizeChange={handlePageSizeChange}
              />
            </Box>
          )}

          {activeTab === 1 && (
            <Box>
              <Box
                sx={{
                  display: "flex",
                  justifyContent: "space-between",
                  alignItems: "center",
                  mb: 3,
                }}
              >
                <Box>
                  <Typography variant="h6" sx={{ fontWeight: 600, mb: 0.5 }}>
                    Scheduled Jobs
                  </Typography>
                  <Typography variant="body2" color="text.secondary">
                    Jobs queued for future execution
                  </Typography>
                </Box>
                <IconButton
                  onClick={() => fetchScheduled()}
                  disabled={scheduledLoading}
                  color="primary"
                >
                  {scheduledLoading ? (
                    <CircularProgress size={20} />
                  ) : (
                    <RefreshIcon />
                  )}
                </IconButton>
              </Box>

              {scheduledLoading ? (
                <Stack spacing={2}>
                  {[1, 2, 3].map((i) => (
                    <Skeleton
                      key={i}
                      variant="rectangular"
                      height={60}
                      sx={{ borderRadius: 1 }}
                    />
                  ))}
                </Stack>
              ) : scheduledJobs.length === 0 ? (
                <Box sx={{ textAlign: "center", py: 8 }}>
                  <ScheduleIcon
                    sx={{ fontSize: 64, color: "text.disabled", mb: 2 }}
                  />
                  <Typography variant="h6" sx={{ mb: 1 }}>
                    No Scheduled Jobs
                  </Typography>
                  <Typography variant="body2" color="text.secondary">
                    There are no jobs waiting for future execution
                  </Typography>
                </Box>
              ) : (
                <ScheduledJobsTable jobs={scheduledJobs} />
              )}

              {/* Scheduled Jobs Pagination */}
              {!scheduledLoading && scheduledJobs.length > 0 && (
                <Box
                  sx={{
                    mt: 3,
                    pt: 2,
                    borderTop: 1,
                    borderColor: "divider",
                    display: "flex",
                    flexDirection: { xs: "column", sm: "row" },
                    alignItems: "center",
                    justifyContent: "space-between",
                    gap: 2,
                  }}
                >
                  <Box sx={{ display: "flex", alignItems: "center", gap: 2 }}>
                    <Typography variant="body2" color="text.secondary">
                      Showing{" "}
                      <strong>
                        {(scheduledPage - 1) * scheduledPageSize + 1}
                      </strong>{" "}
                      to{" "}
                      <strong>
                        {Math.min(
                          scheduledPage * scheduledPageSize,
                          scheduledTotal,
                        )}
                      </strong>{" "}
                      of <strong>{scheduledTotal}</strong> results
                    </Typography>
                    <FormControl size="small" sx={{ minWidth: 100 }}>
                      <Select
                        value={scheduledPageSize}
                        onChange={(e) => {
                          setScheduledPageSize(Number(e.target.value));
                          setScheduledPage(1);
                        }}
                      >
                        <MenuItem value={10}>10</MenuItem>
                        <MenuItem value={20}>20</MenuItem>
                        <MenuItem value={50}>50</MenuItem>
                        <MenuItem value={100}>100</MenuItem>
                      </Select>
                    </FormControl>
                  </Box>
                  <Pagination
                    count={Math.ceil(scheduledTotal / scheduledPageSize)}
                    page={scheduledPage}
                    onChange={(e, p) => {
                      setScheduledPage(p);
                      window.scrollTo({ top: 0, behavior: "smooth" });
                    }}
                    color="primary"
                    size="small"
                    showFirstButton
                    showLastButton
                  />
                </Box>
              )}
            </Box>
          )}
        </Paper>
      </Box>

      {/* Confirmation Modal for Queue Actions */}
      <ConfirmationModal
        isOpen={confirmModal.isOpen}
        onClose={() =>
          setConfirmModal({ isOpen: false, type: null, data: null })
        }
        onConfirm={() => {
          handleModalConfirm(confirmModal.type, {
            handlePauseResume,
            handleBulkRetry,
          });
        }}
        {...getModalConfig(confirmModal.type, queue, confirmModal.data)}
        isLoading={actionLoading || bulkActionLoading}
      />
    </Layout>
  );
}

function PauseResumeButton({ isPaused, actionLoading, onClick }) {
  const { text, color, icon } = getPauseResumeButtonProps(
    isPaused,
    actionLoading,
  );

  return (
    <Button
      onClick={onClick}
      disabled={actionLoading}
      variant="contained"
      color={color}
      startIcon={
        actionLoading ? (
          <CircularProgress size={16} color="inherit" />
        ) : icon === "play" ? (
          <PlayArrowIcon />
        ) : (
          <PauseIcon />
        )
      }
    >
      {text}
    </Button>
  );
}

function StatusBadge({ isPaused }) {
  return (
    <Chip
      icon={isPaused ? <PauseIcon /> : <PlayArrowIcon />}
      label={isPaused ? "Paused" : "Running"}
      color={isPaused ? "warning" : "success"}
      size="small"
      sx={{ fontWeight: 500 }}
    />
  );
}

function MetricCard({ title, value, color, icon }) {
  const colorMap = {
    gray: "default",
    blue: "primary",
    green: "success",
    red: "error",
  };

  const muiColor = colorMap[color] || "default";

  return (
    <Card
      sx={{
        height: "100%",
        display: "flex",
        flexDirection: "column",
        background: (theme) =>
          muiColor === "default"
            ? theme.palette.grey[50]
            : theme.palette[muiColor]?.light || theme.palette.background.paper,
        transition: "box-shadow 0.3s",
        "&:hover": {
          boxShadow: 4,
        },
      }}
    >
      <CardContent
        sx={{ flexGrow: 1, display: "flex", flexDirection: "column" }}
      >
        <Box
          sx={{
            display: "flex",
            justifyContent: "space-between",
            alignItems: "center",
            mb: 2,
          }}
        >
          <Typography
            variant="overline"
            sx={{
              color: (theme) =>
                muiColor === "default"
                  ? theme.palette.text.secondary
                  : theme.palette[muiColor]?.main ||
                    theme.palette.text.secondary,
              fontWeight: 600,
            }}
          >
            {title}
          </Typography>
          <Box
            sx={{
              color: (theme) =>
                muiColor === "default"
                  ? theme.palette.text.secondary
                  : theme.palette[muiColor]?.main ||
                    theme.palette.text.secondary,
            }}
          >
            {icon}
          </Box>
        </Box>
        <Typography
          variant="h4"
          sx={{
            fontWeight: 700,
            color: (theme) =>
              muiColor === "default"
                ? theme.palette.text.primary
                : theme.palette[muiColor]?.dark || theme.palette.text.primary,
          }}
        >
          {typeof value === "number" ? value.toLocaleString() : value}
        </Typography>
      </CardContent>
    </Card>
  );
}
