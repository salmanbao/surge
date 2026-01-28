import { useState, useEffect } from "react";
import {
  Box,
  Typography,
  Grid,
  Card,
  CardContent,
  Chip,
  Alert,
  IconButton,
  Tooltip,
} from "@mui/material";
import {
  People as PeopleIcon,
  Refresh as RefreshIcon,
  CheckCircle as CheckCircleIcon,
} from "@mui/icons-material";
import { Layout } from "./Layout";
import { api } from "../services/api";
import { handleApiError } from "../utils/errorHelpers";
import { SkeletonLoader } from "./SkeletonLoader";

export function WorkersView() {
  const [workers, setWorkers] = useState([]);
  const [loading, setLoading] = useState(true);
  const [lastUpdate, setLastUpdate] = useState(null);

  const fetchWorkers = async () => {
    try {
      const data = await api.getWorkers();
      setWorkers(Array.isArray(data) ? data : []);
      setLoading(false);
      setLastUpdate(new Date());
    } catch (err) {
      handleApiError(err, "Failed to fetch workers");
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchWorkers();
    const interval = setInterval(fetchWorkers, 10000);
    return () => clearInterval(interval);
  }, []);

  if (loading) {
    return (
      <Layout>
        <Box sx={{ maxWidth: "1400px", mx: "auto", p: 3 }}>
          <SkeletonLoader />
        </Box>
      </Layout>
    );
  }

  return (
    <Layout>
      <Box sx={{ maxWidth: "1400px", mx: "auto", p: 3 }}>
        <Box
          sx={{
            mb: 3,
            display: "flex",
            alignItems: "center",
            justifyContent: "space-between",
          }}
        >
          <Box sx={{ display: "flex", alignItems: "center", gap: 2 }}>
            <PeopleIcon sx={{ fontSize: 32, color: "primary.main" }} />
            <Box>
              <Typography variant="h4" component="h1">
                Active Workers
              </Typography>
              <Typography variant="body2" color="text.secondary">
                {workers.length} worker{workers.length !== 1 ? "s" : ""} active
              </Typography>
            </Box>
          </Box>
          <Box sx={{ display: "flex", alignItems: "center", gap: 2 }}>
            {lastUpdate && (
              <Typography variant="caption" color="text.secondary">
                Last updated: {lastUpdate.toLocaleTimeString()}
              </Typography>
            )}
            <Tooltip title="Refresh">
              <IconButton onClick={fetchWorkers} size="small">
                <RefreshIcon />
              </IconButton>
            </Tooltip>
          </Box>
        </Box>

        {workers.length === 0 ? (
          <Alert severity="info" sx={{ maxWidth: 600 }}>
            No active workers found. Workers will appear here when they start
            processing jobs.
          </Alert>
        ) : (
          <Grid container spacing={2}>
            {workers.map((worker, index) => (
              <Grid item xs={12} sm={6} md={4} lg={3} key={index}>
                <Card
                  elevation={2}
                  sx={{
                    height: "100%",
                    transition: "all 0.2s",
                    "&:hover": {
                      transform: "translateY(-4px)",
                      boxShadow: 4,
                    },
                  }}
                >
                  <CardContent>
                    <Box
                      sx={{
                        display: "flex",
                        alignItems: "flex-start",
                        justifyContent: "space-between",
                        mb: 2,
                      }}
                    >
                      <PeopleIcon sx={{ color: "primary.main", fontSize: 28 }} />
                      <Chip
                        icon={<CheckCircleIcon />}
                        label="Active"
                        color="success"
                        size="small"
                        variant="outlined"
                      />
                    </Box>
                    <Typography
                      variant="h6"
                      sx={{
                        fontFamily: "monospace",
                        fontSize: "0.95rem",
                        wordBreak: "break-word",
                        mb: 1,
                      }}
                    >
                      {worker}
                    </Typography>
                    <Typography variant="caption" color="text.secondary">
                      Worker ID
                    </Typography>
                  </CardContent>
                </Card>
              </Grid>
            ))}
          </Grid>
        )}
      </Box>
    </Layout>
  );
}
