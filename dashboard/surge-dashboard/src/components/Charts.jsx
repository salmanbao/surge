import React, { useState, useCallback, useEffect } from "react";
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
} from "recharts";
import {
  Box,
  Typography,
  ToggleButton,
  ToggleButtonGroup,
  TextField,
  CircularProgress,
} from "@mui/material";
import { BarChart as BarChartIcon } from "@mui/icons-material";
import { api } from "../services/api";

const CHART_COLORS = {
  Pending: "#ed6c02",
  Active: "#1976d2",
  Scheduled: "#7b1fa2",
  Processed: "#2e7d32",
  Failed: "#d32f2f",
};

const PRESETS = [
  { label: "7d", days: 7 },
  { label: "14d", days: 14 },
  { label: "30d", days: 30 },
  { label: "90d", days: 90 },
];

function toDateStr(d) {
  return d.toISOString().slice(0, 10);
}

function shortDate(dateStr) {
  const [, m, d] = dateStr.split("-");
  const months = [
    "Jan",
    "Feb",
    "Mar",
    "Apr",
    "May",
    "Jun",
    "Jul",
    "Aug",
    "Sep",
    "Oct",
    "Nov",
    "Dec",
  ];
  return `${months[parseInt(m, 10) - 1]} ${parseInt(d, 10)}`;
}

function EmptyChart({ height = 300 }) {
  return (
    <Box
      sx={{
        height,
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
        color: "text.secondary",
      }}
    >
      <Box sx={{ textAlign: "center" }}>
        <BarChartIcon sx={{ fontSize: 48, mb: 1, opacity: 0.5 }} />
        <Typography variant="body2">No data available</Typography>
      </Box>
    </Box>
  );
}

export function CombinedQueueChart({ queues }) {
  const data = queues.map((q) => ({
    name: q.name,
    Pending: q.stats?.pending || 0,
    Active: q.stats?.processing || 0,
    Scheduled: q.stats?.scheduled || 0,
    Processed: q.stats?.processed || 0,
    Failed: q.stats?.failed || 0,
  }));

  if (data.length === 0) return <EmptyChart />;

  return (
    <Box sx={{ height: 300 }}>
      <ResponsiveContainer width="100%" height="100%">
        <BarChart data={data} barSize={18} key={JSON.stringify(data)}>
          <CartesianGrid
            strokeDasharray="3 3"
            stroke="#e5e7eb"
            vertical={false}
          />
          <XAxis
            dataKey="name"
            stroke="#6b7280"
            fontSize={11}
            tick={{ fill: "#6b7280" }}
          />
          <YAxis stroke="#6b7280" fontSize={11} tick={{ fill: "#6b7280" }} />
          <Tooltip
            contentStyle={{
              backgroundColor: "#fff",
              borderColor: "#e5e7eb",
              color: "#1f2937",
              fontSize: 12,
              borderRadius: "8px",
              boxShadow: "0 4px 6px -1px rgba(0, 0, 0, 0.1)",
            }}
            labelFormatter={(label) => (
              <span style={{ fontWeight: 600 }}>{label}</span>
            )}
          />
          <Legend
            wrapperStyle={{ fontSize: 12 }}
            formatter={(value) => (
              <span
                style={{
                  color: CHART_COLORS[value] || "#1f2937",
                  fontWeight: 500,
                }}
              >
                {value}
              </span>
            )}
          />
          {["Pending", "Active", "Scheduled", "Processed", "Failed"].map(
            (key) => (
              <Bar
                key={key}
                dataKey={key}
                fill={CHART_COLORS[key]}
                name={key}
                isAnimationActive={true}
                radius={[4, 4, 0, 0]}
              />
            ),
          )}
        </BarChart>
      </ResponsiveContainer>
    </Box>
  );
}

export function HistoricalQueueChart({ namespace, queue }) {
  const [preset, setPreset] = useState("7d");
  const [from, setFrom] = useState(() => {
    const d = new Date();
    d.setDate(d.getDate() - 6);
    return toDateStr(d);
  });
  const [to, setTo] = useState(() => toDateStr(new Date()));
  const [data, setData] = useState([]);
  const [loading, setLoading] = useState(true);

  const fetchData = useCallback(
    async (f, t) => {
      if (!namespace || !queue) return;
      setLoading(true);
      try {
        const stats = await api.getQueueStats(namespace, queue, {
          from: f,
          to: t,
        });
        setData(
          (stats.history || []).map((p) => ({
            date: p.date,
            Processed: p.processed,
            Failed: p.failed,
          })),
        );
      } catch (err) {
        console.error("Failed to fetch historical stats:", err);
        setData([]);
      } finally {
        setLoading(false);
      }
    },
    [namespace, queue],
  );

  useEffect(() => {
    fetchData(from, to);
  }, [fetchData, from, to]);

  const handlePreset = (_, p) => {
    if (!p) return;
    setPreset(p);
    const toDate = new Date();
    const fromDate = new Date();
    fromDate.setDate(
      toDate.getDate() - (PRESETS.find((x) => x.label === p)?.days ?? 7) + 1,
    );
    setFrom(toDateStr(fromDate));
    setTo(toDateStr(toDate));
  };

  const isEmpty =
    !loading &&
    (data.length === 0 ||
      data.every((d) => d.Processed === 0 && d.Failed === 0));

  return (
    <Box>
      {/* Controls */}
      <Box
        sx={{
          display: "flex",
          alignItems: "center",
          gap: 2,
          mb: 2,
          flexWrap: "wrap",
        }}
      >
        <ToggleButtonGroup
          value={preset}
          exclusive
          onChange={handlePreset}
          size="small"
        >
          {PRESETS.map((p) => (
            <ToggleButton key={p.label} value={p.label} sx={{ px: 2 }}>
              {p.label}
            </ToggleButton>
          ))}
        </ToggleButtonGroup>

        <Box sx={{ display: "flex", alignItems: "center", gap: 1 }}>
          <TextField
            type="date"
            size="small"
            value={from}
            onChange={(e) => {
              setPreset(null);
              setFrom(e.target.value);
            }}
            inputProps={{ max: to }}
            sx={{ width: 150 }}
          />
          <Typography variant="body2" color="text.secondary">
            to
          </Typography>
          <TextField
            type="date"
            size="small"
            value={to}
            onChange={(e) => {
              setPreset(null);
              setTo(e.target.value);
            }}
            inputProps={{ min: from }}
            sx={{ width: 150 }}
          />
        </Box>
      </Box>

      {/* Chart */}
      {loading ? (
        <Box
          sx={{
            height: 260,
            display: "flex",
            alignItems: "center",
            justifyContent: "center",
          }}
        >
          <CircularProgress size={32} />
        </Box>
      ) : isEmpty ? (
        <Box
          sx={{
            height: 260,
            display: "flex",
            alignItems: "center",
            justifyContent: "center",
            color: "text.secondary",
          }}
        >
          <Box sx={{ textAlign: "center" }}>
            <BarChartIcon sx={{ fontSize: 48, mb: 1, opacity: 0.4 }} />
            <Typography variant="body2">
              No data yet — stats record from this deploy forward.
            </Typography>
          </Box>
        </Box>
      ) : (
        <Box sx={{ height: 260 }}>
          <ResponsiveContainer width="100%" height="100%">
            <BarChart data={data} barSize={16}>
              <CartesianGrid
                strokeDasharray="3 3"
                stroke="#e5e7eb"
                vertical={false}
              />
              <XAxis
                dataKey="date"
                stroke="#6b7280"
                fontSize={11}
                tick={{ fill: "#6b7280" }}
                tickFormatter={shortDate}
              />
              <YAxis
                stroke="#6b7280"
                fontSize={11}
                tick={{ fill: "#6b7280" }}
                allowDecimals={false}
              />
              <Tooltip
                contentStyle={{
                  backgroundColor: "#fff",
                  borderColor: "#e5e7eb",
                  fontSize: 12,
                  borderRadius: "8px",
                  boxShadow: "0 4px 6px -1px rgba(0,0,0,0.1)",
                }}
                labelFormatter={shortDate}
              />
              <Legend wrapperStyle={{ fontSize: 12 }} />
              <Bar
                dataKey="Processed"
                fill={CHART_COLORS.Processed}
                name="Processed"
                radius={[4, 4, 0, 0]}
              />
              <Bar
                dataKey="Failed"
                fill={CHART_COLORS.Failed}
                name="Failed"
                radius={[4, 4, 0, 0]}
              />
            </BarChart>
          </ResponsiveContainer>
        </Box>
      )}
    </Box>
  );
}

// Keep old exports for backward compatibility
export function QueueSizeChart({ queues }) {
  return <CombinedQueueChart queues={queues} />;
}
export function ThroughputChart({ queues }) {
  return <CombinedQueueChart queues={queues} />;
}
