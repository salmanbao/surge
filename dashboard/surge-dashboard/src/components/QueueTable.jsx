import React, { useState, useEffect } from "react";
import { useNavigate, Link } from "react-router-dom";
import toast from "react-hot-toast";
import {
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Paper,
  Chip,
  IconButton,
  Menu,
  MenuItem,
  Skeleton,
  Typography,
  Box,
  Link as MuiLink,
} from "@mui/material";
import {
  MoreVert as MoreVertIcon,
  PlayArrow as PlayArrowIcon,
  Pause as PauseIcon,
  DeleteOutline as DeleteOutlineIcon,
  Description as DescriptionIcon,
  Visibility as VisibilityIcon,
} from "@mui/icons-material";
import { api } from "../services/api";
import { ConfirmationModal } from "./ConfirmationModal";
import { getQueueModalConfig } from "../utils/queueModalConfig";

function formatBytes(bytes, decimals = 2) {
  if (!+bytes) return "0 B";
  const k = 1024;
  const dm = decimals < 0 ? 0 : decimals;
  const sizes = ["B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB"];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return `${parseFloat((bytes / Math.pow(k, i)).toFixed(dm))} ${sizes[i]}`;
}

export function QueueTable({ queues, onStatsUpdate }) {
  return (
    <TableContainer component={Paper} elevation={2}>
      <Table>
        <TableHead>
          <TableRow>
            <TableCell>Queue</TableCell>
            <TableCell>State</TableCell>
            <TableCell align="right">Queued</TableCell>
            <TableCell align="right">Processing</TableCell>
            <TableCell align="right">Data Size</TableCell>
            <TableCell align="right">Processed</TableCell>
            <TableCell align="right">Failed</TableCell>
            <TableCell align="right">Error Rate</TableCell>
            <TableCell align="right">Actions</TableCell>
          </TableRow>
        </TableHead>
        <TableBody>
          {queues.map((queue) => (
            <QueueRow
              key={`${queue.namespace}:${queue.name}`}
              queue={queue}
              stats={queue.stats}
              onStatsUpdate={onStatsUpdate}
            />
          ))}
          {queues.length === 0 && (
            <TableRow>
              <TableCell colSpan={8} align="center" sx={{ py: 4 }}>
                <Typography variant="body2" color="text.secondary">
                  No queues found
                </Typography>
              </TableCell>
            </TableRow>
          )}
        </TableBody>
      </Table>
    </TableContainer>
  );
}

function QueueRow({ queue, onStatsUpdate, stats: propsStats }) {
  const [stats, setStats] = useState(propsStats);
  const [loading, setLoading] = useState(!propsStats);
  const [actionLoading, setActionLoading] = useState(false);
  const [menuAnchor, setMenuAnchor] = useState(null);
  const [confirmModal, setConfirmModal] = useState({
    isOpen: false,
    action: null,
  });
  const navigate = useNavigate();

  useEffect(() => {
    if (propsStats) {
      setStats(propsStats);
      setLoading(false);
    }
  }, [propsStats]);

  const handleMenuOpen = (event) => {
    setMenuAnchor(event.currentTarget);
  };

  const handleMenuClose = () => {
    setMenuAnchor(null);
  };

  const handleActionClick = (action) => {
    handleMenuClose();

    if (action === "details") {
      navigate(
        `/queue/${encodeURIComponent(queue.namespace)}/${encodeURIComponent(queue.name)}`,
      );
      return;
    }

    if (action === "dlq") {
      navigate(
        `/queue/${encodeURIComponent(queue.namespace)}/${encodeURIComponent(queue.name)}`,
        { state: { openDLQ: true } },
      );
      return;
    }

    setConfirmModal({ isOpen: true, action });
  };

  const handleConfirmAction = async () => {
    const { action } = confirmModal;
    if (!action) return;

    setActionLoading(true);
    try {
      switch (action) {
        case "pause":
          await api.pauseQueue(queue.namespace, queue.name);
          toast.success(`Queue "${queue.name}" paused`);
          break;
        case "resume":
          await api.resumeQueue(queue.namespace, queue.name);
          toast.success(`Queue "${queue.name}" resumed`);
          break;
        case "drain":
          try {
            toast.loading("Draining queue...", { id: "drain-" + queue.name });
            const result = await api.drainQueue(queue.namespace, queue.name);
            toast.success(
              `Drained ${result.drained || 0} jobs from ${queue.name}`,
              { id: "drain-" + queue.name },
            );
          } catch (err) {
            toast.dismiss("drain-" + queue.name);
            if (err.message.includes("404")) {
              toast.error(
                "Drain endpoint not found. Please restart your Go server.",
              );
            } else {
              toast.error("Failed to drain: " + err.message);
            }
          }
          break;
        case "dlq":
          toast("DLQ viewer coming soon!", { icon: "📋" });
          break;
      }
      fetchStats();
      setConfirmModal({ isOpen: false, action: null });
    } catch (err) {
      console.error(err);
      toast.error("Action failed: " + err.message);
    } finally {
      setActionLoading(false);
    }
  };

  const fetchStats = async () => {
    try {
      const data = await api.getQueueStats(queue.namespace, queue.name);
      setStats(data);
      if (onStatsUpdate) {
        onStatsUpdate(queue.namespace, queue.name, data);
      }
    } catch (err) {
      console.error("Failed to fetch stats:", err);
    }
  };

  if (loading || !stats) {
    return (
      <TableRow>
        <TableCell>
          <Box>
            <Skeleton variant="text" width={120} height={20} />
            <Skeleton variant="text" width={80} height={16} />
          </Box>
        </TableCell>
        <TableCell>
          <Skeleton
            variant="rectangular"
            width={60}
            height={24}
            sx={{ borderRadius: 1 }}
          />
        </TableCell>
        <TableCell align="right">
          <Skeleton variant="text" width={40} height={20} sx={{ ml: "auto" }} />
        </TableCell>
        <TableCell align="right">
          <Skeleton variant="text" width={40} height={20} sx={{ ml: "auto" }} />
        </TableCell>
        <TableCell align="right">
          <Skeleton variant="text" width={50} height={20} sx={{ ml: "auto" }} />
        </TableCell>
        <TableCell align="right">
          <Skeleton variant="text" width={40} height={20} sx={{ ml: "auto" }} />
        </TableCell>
        <TableCell align="right">
          <Skeleton variant="text" width={50} height={20} sx={{ ml: "auto" }} />
        </TableCell>
        <TableCell align="right">
          <Skeleton
            variant="circular"
            width={32}
            height={32}
            sx={{ ml: "auto" }}
          />
        </TableCell>
      </TableRow>
    );
  }

  const total = (stats.processed || 0) + (stats.failed || 0);
  const errorRate =
    total > 0 ? ((stats.failed / total) * 100).toFixed(2) : "0.00";
  const isPaused = stats.paused === true;

  return (
    <TableRow hover>
      <TableCell>
        <Box>
          <MuiLink
            component={Link}
            to={`/queue/${encodeURIComponent(queue.namespace)}/${encodeURIComponent(queue.name)}`}
            sx={{
              fontWeight: 500,
              color: "primary.main",
              textDecoration: "none",
              "&:hover": {
                textDecoration: "underline",
              },
            }}
          >
            {queue.name}
          </MuiLink>
          <Typography variant="caption" color="text.secondary" display="block">
            {queue.namespace}
          </Typography>
        </Box>
      </TableCell>

      <TableCell>
        <Chip
          label={isPaused ? "Paused" : "Running"}
          color={isPaused ? "warning" : "success"}
          size="small"
          sx={{ fontWeight: 500 }}
        />
      </TableCell>

      <TableCell align="right">
        <Typography variant="body2" fontFamily="monospace">
          {stats.pending}
        </Typography>
      </TableCell>

      <TableCell align="right">
        <Typography
          variant="body2"
          fontFamily="monospace"
          color="primary.main"
          fontWeight={600}
        >
          {stats.processing}
        </Typography>
      </TableCell>

      <TableCell align="right">
        <Typography variant="body2" fontFamily="monospace">
          {formatBytes(stats.memory_usage || 0)}
        </Typography>
      </TableCell>

      <TableCell align="right">
        <Typography variant="body2" fontFamily="monospace">
          {stats.processed || 0}
        </Typography>
      </TableCell>

      <TableCell align="right">
        <Typography
          variant="body2"
          fontFamily="monospace"
          color="error.main"
          fontWeight={600}
        >
          {stats.failed}
        </Typography>
      </TableCell>

      <TableCell align="right">
        <Typography variant="body2" fontFamily="monospace">
          {errorRate}%
        </Typography>
      </TableCell>

      <TableCell align="right">
        <IconButton
          size="small"
          onClick={handleMenuOpen}
          aria-label="Queue actions"
        >
          <MoreVertIcon />
        </IconButton>

        <Menu
          anchorEl={menuAnchor}
          open={Boolean(menuAnchor)}
          onClose={handleMenuClose}
          anchorOrigin={{
            vertical: "bottom",
            horizontal: "right",
          }}
          transformOrigin={{
            vertical: "top",
            horizontal: "right",
          }}
        >
          <MenuItem
            onClick={() => handleActionClick(isPaused ? "resume" : "pause")}
          >
            {isPaused ? (
              <>
                <PlayArrowIcon sx={{ mr: 1, fontSize: 20 }} />
                Resume
              </>
            ) : (
              <>
                <PauseIcon sx={{ mr: 1, fontSize: 20 }} />
                Pause
              </>
            )}
          </MenuItem>
          <MenuItem onClick={() => handleActionClick("drain")}>
            <DeleteOutlineIcon sx={{ mr: 1, fontSize: 20 }} />
            Drain Queue
          </MenuItem>
          <MenuItem onClick={() => handleActionClick("dlq")}>
            <DescriptionIcon sx={{ mr: 1, fontSize: 20 }} />
            View DLQ
          </MenuItem>
          <MenuItem onClick={() => handleActionClick("details")}>
            <VisibilityIcon sx={{ mr: 1, fontSize: 20 }} />
            View Details
          </MenuItem>
        </Menu>
      </TableCell>

      <ConfirmationModal
        isOpen={confirmModal.isOpen}
        onClose={() => setConfirmModal({ isOpen: false, action: null })}
        onConfirm={handleConfirmAction}
        isLoading={actionLoading}
        {...getQueueModalConfig(confirmModal.action, queue.name)}
      />
    </TableRow>
  );
}
