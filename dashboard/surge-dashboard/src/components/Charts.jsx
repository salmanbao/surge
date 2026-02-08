import React from "react";
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
import { Box, Typography } from "@mui/material";
import { BarChart as BarChartIcon } from "@mui/icons-material";

export function CombinedQueueChart({ queues }) {
  const data = queues.map((q) => ({
    name: q.name,
    Pending: q.stats?.pending || 0,
    Active: q.stats?.processing || 0,
    Processed: q.stats?.processed || 0,
    Failed: q.stats?.failed || 0,
  }));

  if (data.length === 0) {
    return (
      <Box
        sx={{
          height: 300,
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
            itemStyle={(entry) => {
              const colors = {
                Pending: "#ed6c02",
                Active: "#1976d2",
                Processed: "#2e7d32",
                Failed: "#d32f2f",
              };
              return {
                color: colors[entry.name] || "#1f2937",
                fontWeight: 600,
              };
            }}
          />
          <Legend
            wrapperStyle={{ fontSize: 12 }}
            formatter={(value) => {
              const colors = {
                Pending: "#ed6c02",
                Active: "#1976d2",
                Processed: "#2e7d32",
                Failed: "#d32f2f",
              };
              return (
                <span
                  style={{ color: colors[value] || "#1f2937", fontWeight: 500 }}
                >
                  {value}
                </span>
              );
            }}
          />
          <Bar
            dataKey="Pending"
            fill="#ed6c02"
            name="Pending"
            isAnimationActive={true}
            radius={[4, 4, 0, 0]}
          />
          <Bar
            dataKey="Active"
            fill="#1976d2"
            name="Active"
            isAnimationActive={true}
            radius={[4, 4, 0, 0]}
          />
          <Bar
            dataKey="Processed"
            fill="#2e7d32"
            name="Processed"
            isAnimationActive={true}
            radius={[4, 4, 0, 0]}
          />
          <Bar
            dataKey="Failed"
            fill="#d32f2f"
            name="Failed"
            isAnimationActive={true}
            radius={[4, 4, 0, 0]}
          />
        </BarChart>
      </ResponsiveContainer>
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
