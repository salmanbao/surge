import React, {
  createContext,
  useContext,
  useState,
  useEffect,
  useCallback,
} from "react";
import { useParams, useLocation } from "react-router-dom";
import toast from "react-hot-toast";
import { api, API_BASE } from "../services/api";

const QueueDetailsContext = createContext(null);

export function QueueDetailsProvider({ children }) {
  const { namespace, queue } = useParams();
  const location = useLocation();

  // Dashboard State
  const [stats, setStats] = useState(null);
  const [loading, setLoading] = useState(true);
  const [lastUpdate, setLastUpdate] = useState(null);
  const [actionLoading, setActionLoading] = useState(false);
  const [confirmModal, setConfirmModal] = useState({
    isOpen: false,
    type: null,
    data: null,
  });

  // Tabs & View State
  const [activeTab, setActiveTab] = useState(location.state?.openDLQ ? 0 : 0);
  const [viewMode, setViewMode] = useState("table");
  const [searchQuery, setSearchQuery] = useState("");

  // DLQ (Failed Jobs) State
  const [dlqJobs, setDlqJobs] = useState([]);
  const [dlqLoading, setDlqLoading] = useState(true);
  const [dlqPage, setDlqPage] = useState(1);
  const [dlqPageSize, setDlqPageSize] = useState(20);
  const [dlqTotal, setDlqTotal] = useState(0);
  const [dlqInitialLoad, setDlqInitialLoad] = useState(true);
  const [selectedJobs, setSelectedJobs] = useState(new Set());
  const [bulkActionLoading, setBulkActionLoading] = useState(false);

  // Scheduled Jobs State
  const [scheduledJobs, setScheduledJobs] = useState([]);
  const [scheduledLoading, setScheduledLoading] = useState(false);
  const [scheduledPage, setScheduledPage] = useState(1);
  const [scheduledPageSize, setScheduledPageSize] = useState(20);
  const [scheduledTotal, setScheduledTotal] = useState(0);

  // --- Actions ---

  const fetchStats = useCallback(() => {
    api
      .getQueueStats(namespace, queue)
      .then((data) => {
        setStats(data);
        setLoading(false);
        setLastUpdate(new Date());
      })
      .catch((err) => {
        console.error(err);
        setLoading(false);
      });
  }, [namespace, queue]);

  // SSE Subscription
  useEffect(() => {
    fetchStats();
    setDlqPage(1);
    // fetchDLQ called via effect below

    const eventSource = new EventSource(
      `${API_BASE}/queue/stats/stream?namespace=${encodeURIComponent(namespace)}&queue=${encodeURIComponent(queue)}`,
    );

    eventSource.onmessage = (event) => {
      try {
        const data = JSON.parse(event.data);
        setStats(data);
        setLoading(false);
        setLastUpdate(new Date());
      } catch (err) {
        console.error("SSE parse error:", err);
      }
    };

    eventSource.onerror = (err) => {
      console.error("SSE error:", err);
      eventSource.close();
    };

    return () => eventSource.close();
  }, [namespace, queue, fetchStats]);

  // Update DLQ total when stats change
  useEffect(() => {
    if (stats) setDlqTotal(stats.failed || 0);
  }, [stats]);

  const fetchDLQ = useCallback(
    (page = dlqPage, pageSize = dlqPageSize) => {
      setDlqLoading(true);
      const offset = (page - 1) * pageSize;
      api
        .getDLQ(namespace, queue, offset, pageSize)
        .then((data) => {
          setDlqJobs(data || []);
          setDlqTotal(stats?.failed || 0);
          setDlqLoading(false);
          setSelectedJobs(new Set());
        })
        .catch((err) => {
          console.error(err);
          setDlqLoading(false);
        });
    },
    [namespace, queue, dlqPage, dlqPageSize, stats?.failed],
  );

  const fetchScheduled = useCallback(
    (page = scheduledPage, pageSize = scheduledPageSize) => {
      setScheduledLoading(true);
      const offset = (page - 1) * pageSize;
      api
        .getScheduledJobs(namespace, queue, offset, pageSize)
        .then((data) => {
          setScheduledJobs(data.jobs || []);
          setScheduledTotal(data.total || 0);
          setScheduledLoading(false);
        })
        .catch((err) => {
          console.error(err);
          setScheduledLoading(false);
        });
    },
    [namespace, queue, scheduledPage, scheduledPageSize],
  );

  useEffect(() => {
    if (activeTab === 0) {
      if (dlqInitialLoad) {
        setDlqInitialLoad(false);
        fetchDLQ(1, dlqPageSize);
      } else {
        fetchDLQ(dlqPage, dlqPageSize);
      }
    }
  }, [dlqPage, dlqPageSize, fetchDLQ, activeTab, dlqInitialLoad]);

  useEffect(() => {
    fetchScheduled(1, scheduledPageSize);
  }, [namespace, queue, fetchScheduled, scheduledPageSize]);

  useEffect(() => {
    if (activeTab === 1) {
      fetchScheduled(scheduledPage, scheduledPageSize);
    }
  }, [activeTab, scheduledPage, scheduledPageSize, fetchScheduled]);

  // Handlers
  const handleTabChange = (event, newValue) => setActiveTab(newValue);

  const toggleJobSelection = (jobId) => {
    const newSelected = new Set(selectedJobs);
    if (newSelected.has(jobId)) newSelected.delete(jobId);
    else newSelected.add(jobId);
    setSelectedJobs(newSelected);
  };

  const selectAll = (filteredJobs) => {
    if (selectedJobs.size === filteredJobs.length) {
      setSelectedJobs(new Set());
    } else {
      setSelectedJobs(new Set(filteredJobs.map((j) => j.id || j.ID)));
    }
  };

  const handleBulkRetry = async () => {
    if (selectedJobs.size === 0) return;
    setBulkActionLoading(true);
    const jobIds = Array.from(selectedJobs);
    let successCount = 0;
    let failCount = 0;

    for (const jobId of jobIds) {
      try {
        await api.retryJob(jobId);
        successCount++;
      } catch (err) {
        failCount++;
        console.error(`Failed to retry job ${jobId}:`, err);
      }
    }

    if (successCount > 0)
      toast.success(
        `Retried ${successCount} job${successCount !== 1 ? "s" : ""}`,
      );
    if (failCount > 0)
      toast.error(
        `Failed to retry ${failCount} job${failCount !== 1 ? "s" : ""}`,
      );

    setSelectedJobs(new Set());
    setBulkActionLoading(false);
    setConfirmModal({ isOpen: false, type: null, data: null });
    fetchDLQ();
  };

  const handlePauseResume = async () => {
    if (!stats) return;
    setActionLoading(true);
    try {
      if (stats.paused) {
        await api.resumeQueue(namespace, queue);
        toast.success("Queue resumed successfully");
      } else {
        await api.pauseQueue(namespace, queue);
        toast.success("Queue paused successfully");
      }
      fetchStats();
      setConfirmModal({ isOpen: false, type: null, data: null });
    } catch (err) {
      toast.error(
        `Failed to ${stats.paused ? "resume" : "pause"} queue: ${err.message}`,
      );
    } finally {
      setActionLoading(false);
    }
  };

  const value = {
    namespace,
    queue,
    stats,
    loading,
    lastUpdate,
    activeTab,
    setActiveTab,
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
    setSelectedJobs,
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
  };

  return (
    <QueueDetailsContext.Provider value={value}>
      {children}
    </QueueDetailsContext.Provider>
  );
}

// eslint-disable-next-line react-refresh/only-export-components
export const useQueueDetails = () => {
  const context = useContext(QueueDetailsContext);
  if (!context) {
    throw new Error(
      "useQueueDetails must be used within a QueueDetailsProvider",
    );
  }
  return context;
};
