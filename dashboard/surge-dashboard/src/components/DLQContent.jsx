import {
  Box,
  Typography,
  Stack,
  Skeleton,
  TextField,
  InputAdornment,
  ToggleButtonGroup,
  ToggleButton,
  Button,
  CircularProgress,
  FormControl,
  Select,
  MenuItem,
  Pagination,
} from "@mui/material";
import {
  Search as SearchIcon,
  TableChart as TableChartIcon,
  ViewModule as ViewModuleIcon,
  CheckCircle as CheckCircleIcon,
  Replay as RetryIcon,
} from "@mui/icons-material";
import { FailedJobsTable } from "./tables/FailedJobsTable";
import { JobCard } from "./JobCard";

export function DLQContent({
  loading,
  jobs,
  filteredJobs,
  searchQuery,
  onSearchChange,
  viewMode,
  onViewModeChange,
  selectedJobs,
  onToggleSelection,
  onSelectAll,
  onRetry,
  onBulkRetryClick,
  bulkActionLoading,
  page,
  pageSize,
  total,
  onPageChange,
  onPageSizeChange,
}) {
  if (loading) {
    return (
      <Stack spacing={2}>
        {[1, 2, 3].map((i) => (
          <Skeleton
            key={i}
            variant="rectangular"
            height={96}
            sx={{ borderRadius: 1 }}
          />
        ))}
      </Stack>
    );
  }

  if (filteredJobs.length === 0) {
    return <EmptyState hasSearch={!!searchQuery} />;
  }

  return (
    <>
      <SearchAndViewControls
        searchQuery={searchQuery}
        onSearchChange={onSearchChange}
        viewMode={viewMode}
        onViewModeChange={onViewModeChange}
        showControls={!loading && jobs.length > 0}
      />

      <BulkActionBar
        selectedCount={selectedJobs.size}
        onBulkRetry={onBulkRetryClick}
        loading={bulkActionLoading}
        show={!loading && filteredJobs.length > 0 && selectedJobs.size > 0}
      />

      <JobListView
        jobs={filteredJobs}
        viewMode={viewMode}
        selectedJobs={selectedJobs}
        onToggleSelection={onToggleSelection}
        onSelectAll={onSelectAll}
        onRetry={onRetry}
      />

      <PaginationControls
        page={page}
        pageSize={pageSize}
        total={total}
        onPageChange={onPageChange}
        onPageSizeChange={onPageSizeChange}
        show={!loading && filteredJobs.length > 0}
      />
    </>
  );
}

function EmptyState({ hasSearch }) {
  if (hasSearch) {
    return (
      <Box sx={{ textAlign: "center", py: 8 }}>
        <SearchIcon sx={{ fontSize: 64, color: "text.disabled", mb: 2 }} />
        <Typography variant="h6" sx={{ mb: 1 }}>
          No jobs found
        </Typography>
        <Typography variant="body2" color="text.secondary">
          Try adjusting your search query
        </Typography>
      </Box>
    );
  }

  return (
    <Box sx={{ textAlign: "center", py: 8 }}>
      <CheckCircleIcon sx={{ fontSize: 64, color: "success.main", mb: 2 }} />
      <Typography variant="h6" sx={{ mb: 1 }}>
        No Failed Jobs
      </Typography>
      <Typography variant="body2" color="text.secondary">
        All jobs are processing successfully
      </Typography>
    </Box>
  );
}

function SearchAndViewControls({
  searchQuery,
  onSearchChange,
  viewMode,
  onViewModeChange,
  showControls,
}) {
  if (!showControls) return null;

  return (
    <Box
      sx={{
        display: "flex",
        justifyContent: "space-between",
        mb: 2,
        gap: 2,
        flexWrap: "wrap",
      }}
    >
      <TextField
        placeholder="Search by job ID, error, or topic..."
        value={searchQuery}
        onChange={(e) => onSearchChange(e.target.value)}
        size="small"
        sx={{ flex: 1, minWidth: 300 }}
        InputProps={{
          startAdornment: (
            <InputAdornment position="start">
              <SearchIcon />
            </InputAdornment>
          ),
        }}
      />
      <ToggleButtonGroup
        value={viewMode}
        exclusive
        onChange={(e, newMode) => newMode && onViewModeChange(newMode)}
        size="small"
      >
        <ToggleButton value="table">
          <TableChartIcon sx={{ mr: 0.5, fontSize: 18 }} />
          Table
        </ToggleButton>
        <ToggleButton value="cards">
          <ViewModuleIcon sx={{ mr: 0.5, fontSize: 18 }} />
          Cards
        </ToggleButton>
      </ToggleButtonGroup>
    </Box>
  );
}

function BulkActionBar({ selectedCount, onBulkRetry, loading, show }) {
  if (!show) return null;

  return (
    <Box
      sx={{
        mb: 2,
        p: 2,
        bgcolor: "primary.light",
        borderRadius: 1,
        display: "flex",
        alignItems: "center",
        justifyContent: "space-between",
        border: "1px solid",
        borderColor: "primary.main",
      }}
    >
      <Typography variant="body1" sx={{ fontWeight: 500, color: "primary.contrastText" }}>
        {selectedCount} job{selectedCount !== 1 ? "s" : ""} selected
      </Typography>
      <Button
        onClick={onBulkRetry}
        disabled={loading}
        variant="contained"
        color="primary"
        size="medium"
        startIcon={loading ? <CircularProgress size={16} color="inherit" /> : <RetryIcon />}
        sx={{
          bgcolor: "primary.dark",
          "&:hover": {
            bgcolor: "primary.dark",
            opacity: 0.9,
          },
        }}
      >
        {loading ? "Retrying..." : `Retry ${selectedCount} Selected`}
      </Button>
    </Box>
  );
}

function JobListView({
  jobs,
  viewMode,
  selectedJobs,
  onToggleSelection,
  onSelectAll,
  onRetry,
}) {
  if (viewMode === "table") {
    return (
      <FailedJobsTable
        jobs={jobs}
        selectedJobs={selectedJobs}
        onToggleSelection={onToggleSelection}
        onSelectAll={onSelectAll}
        onRetry={onRetry}
      />
    );
  }

  return (
    <Stack spacing={2}>
      {jobs.map((job, idx) => (
        <JobCard
          key={job.id || idx}
          job={job}
          onRetry={onRetry}
          selected={selectedJobs.has(job.id)}
          onToggleSelection={() => onToggleSelection(job.id)}
        />
      ))}
    </Stack>
  );
}

function PaginationControls({
  page,
  pageSize,
  total,
  onPageChange,
  onPageSizeChange,
  show,
}) {
  if (!show) return null;

  const totalPages = Math.ceil(total / pageSize);

  return (
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
          Showing <strong>{(page - 1) * pageSize + 1}</strong> to{" "}
          <strong>{Math.min(page * pageSize, total)}</strong> of{" "}
          <strong>{total}</strong> results
        </Typography>
        <FormControl size="small" sx={{ minWidth: 100 }}>
          <Select
            value={pageSize}
            onChange={(e) => onPageSizeChange(Number(e.target.value))}
          >
            <MenuItem value={10}>10</MenuItem>
            <MenuItem value={20}>20</MenuItem>
            <MenuItem value={50}>50</MenuItem>
            <MenuItem value={100}>100</MenuItem>
          </Select>
        </FormControl>
      </Box>
      {totalPages > 1 && (
        <Pagination
          count={totalPages}
          page={page}
          onChange={onPageChange}
          color="primary"
          size="small"
          showFirstButton
          showLastButton
        />
      )}
    </Box>
  );
}
