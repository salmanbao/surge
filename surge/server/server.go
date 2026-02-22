package server

import (
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/olamilekan000/surge/surge/backend"
)

//go:embed dashboard-dist/*
var dashboardFS embed.FS

type BackendProvider interface {
	Backend() backend.Backend
	GetRegisteredHandlers(ctx context.Context) ([]string, error)
}

type DashboardServer struct {
	Backend  backend.Backend
	Provider BackendProvider
	Port     int
	RootPath string
}

func NewDashboardServer(provider BackendProvider, port int) *DashboardServer {
	return &DashboardServer{
		Backend:  provider.Backend(),
		Provider: provider,
		Port:     port,
		RootPath: "/",
	}
}

// SetRootPath sets the root path for the dashboard.
// This should match where you mount the handler.
// Example: dashboard.SetRootPath("/queue/monitoring")
func (s *DashboardServer) SetRootPath(path string) {
	if path == "" {
		path = "/"
	}
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	if !strings.HasSuffix(path, "/") {
		path += "/"
	}
	s.RootPath = path
}

// Handler returns an http.Handler that can be mounted on any HTTP server.
// Mount this handler with a wildcard to catch all sub-routes:
//
//	dashboard.SetRootPath("/queue/monitoring")
//	mux.Handle("/queue/monitoring/*", dashboard.Handler())
func (s *DashboardServer) Handler() http.Handler {
	mux := http.NewServeMux()
	rootPath := s.RootPath

	mux.HandleFunc(rootPath+"api/queues", jsonResponse(s.handleGetQueues))
	mux.HandleFunc(rootPath+"api/queue/stats", jsonResponse(s.handleGetQueueStats))
	mux.HandleFunc(rootPath+"api/queue/stats/batch", jsonResponse(s.handleGetBatchQueueStats))
	mux.HandleFunc(rootPath+"api/queue/stats/stream", s.handleQueueStatsStream)
	mux.HandleFunc(rootPath+"api/dlq", jsonResponse(s.handleGetDLQ))
	mux.HandleFunc(rootPath+"api/queue/scheduled", jsonResponse(s.handleGetScheduledJobs))
	mux.HandleFunc(rootPath+"api/namespaces", jsonResponse(s.handleGetNamespaces))
	mux.HandleFunc(rootPath+"api/handlers", jsonResponse(s.handleGetHandlers))
	mux.HandleFunc(rootPath+"api/workers", jsonResponse(s.handleGetWorkers))
	mux.HandleFunc(rootPath+"api/queue/pause", jsonResponse(s.handlePauseQueue))
	mux.HandleFunc(rootPath+"api/queue/resume", jsonResponse(s.handleResumeQueue))
	mux.HandleFunc(rootPath+"api/queue/drain", jsonResponse(s.handleDrainQueue))
	mux.HandleFunc(rootPath+"api/dlq/retry", jsonResponse(s.handleRetryFromDLQ))

	s.serveStaticFiles(mux, rootPath)

	return mux
}

func jsonResponse(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		next(w, r)
	}
}

func normalizePath(path string) string {
	if path == "" {
		return "/"
	}
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	return path
}

func (s *DashboardServer) serveStaticFiles(mux *http.ServeMux, rootPath string) {
	fsys, err := fs.Sub(dashboardFS, "dashboard-dist")
	if err != nil {
		panic(fmt.Sprintf("failed to load embedded dashboard files: %v. Run ./scripts/build-dashboard.sh to build the dashboard", err))
	}

	fileServer := http.FileServer(http.FS(fsys))

	mux.HandleFunc(rootPath, func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, rootPath+"api/") {
			http.NotFound(w, r)
			return
		}

		path := strings.TrimPrefix(r.URL.Path, strings.TrimSuffix(rootPath, "/"))
		path = normalizePath(path)

		if path != "/" && strings.Contains(path, ".") && !strings.HasSuffix(path, "/") {
			newReq := r.Clone(r.Context())
			newReq.URL.Path = path
			fileServer.ServeHTTP(w, newReq)
			return
		}

		indexFile, err := fsys.Open("index.html")
		if err != nil {
			http.NotFound(w, r)
			return
		}
		defer indexFile.Close()

		content, err := io.ReadAll(indexFile)
		if err != nil {
			http.Error(w, "Failed to read index.html", http.StatusInternalServerError)
			return
		}

		html := string(content)

		configScript := fmt.Sprintf(`<script>window.SURGE_DASHBOARD_CONFIG = { basename: "%s" };</script>`, rootPath)
		baseTag := fmt.Sprintf(`<base href="%s">`, rootPath)

		injection := baseTag + configScript

		html = strings.Replace(html, "<head>", "<head>"+injection, 1)

		w.Header().Set("Content-Type", "text/html")
		w.Write([]byte(html))
	})
}

func (s *DashboardServer) Start() error {
	addr := fmt.Sprintf(":%d", s.Port)
	fmt.Printf("Dashboard API listening on %s\n", addr)
	return http.ListenAndServe(addr, s.Handler())
}

func (s *DashboardServer) handleGetNamespaces(w http.ResponseWriter, r *http.Request) {
	namespaces, err := s.Backend.GetNamespaces(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	json.NewEncoder(w).Encode(namespaces)
}

func (s *DashboardServer) handleGetHandlers(w http.ResponseWriter, r *http.Request) {
	if s.Provider == nil {
		json.NewEncoder(w).Encode([]string{})
		return
	}

	handlers, err := s.Provider.GetRegisteredHandlers(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	json.NewEncoder(w).Encode(handlers)
}

func (s *DashboardServer) handleGetWorkers(w http.ResponseWriter, r *http.Request) {
	workers, err := s.Backend.GetActiveWorkers(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	json.NewEncoder(w).Encode(workers)
}

func (s *DashboardServer) handleRetryFromDLQ(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	jobID := r.URL.Query().Get("job_id")
	if jobID == "" {
		http.Error(w, "job_id is required", http.StatusBadRequest)
		return
	}

	err := s.Backend.RetryFromDLQ(r.Context(), jobID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	json.NewEncoder(w).Encode(map[string]string{"status": "success", "job_id": jobID})
}

func (s *DashboardServer) handleGetQueues(w http.ResponseWriter, r *http.Request) {
	queues, err := s.Backend.DiscoverQueues(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	filterNs := r.URL.Query().Get("namespace")

	response := make([]map[string]string, 0)
	for _, q := range queues {
		parts := strings.Split(q, ":")
		if len(parts) >= 4 {
			ns := parts[1]
			if filterNs != "" && filterNs != "all" && ns != filterNs {
				continue
			}

			response = append(response, map[string]string{
				"name":      parts[3],
				"namespace": ns,
				"full_key":  q,
			})
		}
	}

	json.NewEncoder(w).Encode(response)
}

func (s *DashboardServer) handleGetQueueStats(w http.ResponseWriter, r *http.Request) {
	namespace := r.URL.Query().Get("namespace")
	queue := r.URL.Query().Get("queue")

	if namespace == "" || queue == "" {
		http.Error(w, "namespace and queue required", http.StatusBadRequest)
		return
	}

	stats, err := s.Backend.QueueStats(r.Context(), namespace, queue, time.Time{}, time.Time{})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	json.NewEncoder(w).Encode(stats)
}

func (s *DashboardServer) handleGetBatchQueueStats(w http.ResponseWriter, r *http.Request) {
	namespace := r.URL.Query().Get("namespace")
	if namespace == "" {
		namespace = "default"
	}

	today := time.Now().UTC().Format("2006-01-02")
	fromStr := r.URL.Query().Get("from")
	toStr := r.URL.Query().Get("to")
	if fromStr == "" {
		fromStr = today
	}
	if toStr == "" {
		toStr = today
	}

	from, _ := time.Parse("2006-01-02", fromStr)
	to, _ := time.Parse("2006-01-02", toStr)

	queues, err := s.Backend.DiscoverQueues(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	type QueueStatsResponse struct {
		Queue     string              `json:"queue"`
		Namespace string              `json:"namespace"`
		Stats     *backend.QueueStats `json:"stats"`
	}

	var results []QueueStatsResponse
	for _, q := range queues {
		parts := strings.Split(q, ":")
		if len(parts) < 4 {
			continue
		}
		queueNs := parts[1]
		queueName := parts[3]

		if queueNs != namespace {
			continue
		}

		stats, err := s.Backend.QueueStats(r.Context(), queueNs, queueName, from, to)
		if err != nil {
			continue
		}

		results = append(results, QueueStatsResponse{
			Queue:     queueName,
			Namespace: queueNs,
			Stats:     stats,
		})
	}

	json.NewEncoder(w).Encode(results)
}

func (s *DashboardServer) handleQueueStatsStream(w http.ResponseWriter, r *http.Request) {
	namespace := r.URL.Query().Get("namespace")
	queue := r.URL.Query().Get("queue")

	if namespace == "" {
		namespace = "default"
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "Streaming unsupported", http.StatusInternalServerError)
		return
	}

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	ctx := r.Context()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			stats, err := s.Backend.QueueStats(ctx, namespace, queue, time.Time{}, time.Time{})
			if err != nil {
				continue
			}

			data, err := json.Marshal(stats)
			if err != nil {
				continue
			}

			fmt.Fprintf(w, "data: %s\n\n", data)
			flusher.Flush()
		}
	}
}

func (s *DashboardServer) handleGetScheduledJobs(w http.ResponseWriter, r *http.Request) {
	namespace := r.URL.Query().Get("namespace")
	queue := r.URL.Query().Get("queue")
	offsetStr := r.URL.Query().Get("offset")
	limitStr := r.URL.Query().Get("limit")

	if namespace == "" || queue == "" {
		http.Error(w, "namespace and queue required", http.StatusBadRequest)
		return
	}

	offset, _ := strconv.Atoi(offsetStr)
	limit, _ := strconv.Atoi(limitStr)
	if limit == 0 {
		limit = 10
	}

	jobs, total, err := s.Backend.GetScheduledJobs(r.Context(), namespace, queue, offset, limit)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Wrapper for total count + data
	response := map[string]any{
		"jobs":  jobs,
		"total": total,
	}

	json.NewEncoder(w).Encode(response)
}

func (s *DashboardServer) handleGetDLQ(w http.ResponseWriter, r *http.Request) {
	namespace := r.URL.Query().Get("namespace")
	queue := r.URL.Query().Get("queue")
	offsetStr := r.URL.Query().Get("offset")
	limitStr := r.URL.Query().Get("limit")

	if namespace == "" {
		namespace = "default"
	}

	offset, _ := strconv.Atoi(offsetStr)
	limit, _ := strconv.Atoi(limitStr)
	if limit == 0 {
		limit = 10
	}

	jobs, err := s.Backend.InspectDLQ(r.Context(), namespace, queue, offset, limit)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	json.NewEncoder(w).Encode(jobs)
}

func (s *DashboardServer) handlePauseQueue(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	namespace := r.URL.Query().Get("namespace")
	queue := r.URL.Query().Get("queue")

	if namespace == "" || queue == "" {
		http.Error(w, "namespace and queue required", http.StatusBadRequest)
		return
	}

	if err := s.Backend.Pause(r.Context(), namespace, queue); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func (s *DashboardServer) handleResumeQueue(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	namespace := r.URL.Query().Get("namespace")
	queue := r.URL.Query().Get("queue")

	if namespace == "" || queue == "" {
		http.Error(w, "namespace and queue required", http.StatusBadRequest)
		return
	}

	if err := s.Backend.Resume(r.Context(), namespace, queue); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func (s *DashboardServer) handleDrainQueue(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	namespace := r.URL.Query().Get("namespace")
	queue := r.URL.Query().Get("queue")

	if namespace == "" || queue == "" {
		http.Error(w, "namespace and queue required", http.StatusBadRequest)
		return
	}

	count, err := s.Backend.Drain(r.Context(), namespace, queue)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	json.NewEncoder(w).Encode(map[string]int64{"drained": count})
}
