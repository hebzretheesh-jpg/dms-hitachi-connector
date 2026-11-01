package metrics

import (
	"fmt"
	"sort"
	"strings"
	"sync"
)

type Registry struct {
	mu sync.Mutex

	requests  map[string]int64
	durations map[string]float64

	pingSuccess           int64
	pingFailure           int64
	hitachiConnectionFail int64

	hitachiUploadFail       map[string]int64
	hitachiUploadSuccess    map[string]int64
	hitachiGetStreamFail    map[string]int64
	hitachiGetStreamSuccess map[string]int64
	fileUploadSuccess       map[string]int64
	fileUploadFail          map[string]int64
	fileDownloadSuccess     map[string]int64
	fileDownloadFail        map[string]int64
	getStatsFail            map[string]int64
	getStatsSuccess         map[string]int64
	getPDFImageSuccess      map[string]int64
	getPDFImageFail         map[string]int64
}

func New() *Registry {
	return &Registry{
		requests:                map[string]int64{},
		durations:               map[string]float64{},
		hitachiUploadFail:       map[string]int64{},
		hitachiUploadSuccess:    map[string]int64{},
		hitachiGetStreamFail:    map[string]int64{},
		hitachiGetStreamSuccess: map[string]int64{},
		fileUploadSuccess:       map[string]int64{},
		fileUploadFail:          map[string]int64{},
		fileDownloadSuccess:     map[string]int64{},
		fileDownloadFail:        map[string]int64{},
		getStatsFail:            map[string]int64{},
		getStatsSuccess:         map[string]int64{},
		getPDFImageSuccess:      map[string]int64{},
		getPDFImageFail:         map[string]int64{},
	}
}

func key(parts ...string) string {
	return strings.Join(parts, "|")
}

func (r *Registry) Observe(endpoint, method string, status int, seconds float64) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.requests[key(endpoint, method, fmt.Sprint(status))]++
	r.durations[key(endpoint, method)] += seconds
}

func (r *Registry) IncPingSuccess() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.pingSuccess++
}

func (r *Registry) IncPingFailure() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.pingFailure++
}

func (r *Registry) IncHitachiConnectionFail() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.hitachiConnectionFail++
}

func (r *Registry) IncHitachiUploadFail(bucketName string) {
	r.incByBucket(&r.mu, r.hitachiUploadFail, bucketName)
}

func (r *Registry) IncHitachiUploadSuccess(bucketName string) {
	r.incByBucket(&r.mu, r.hitachiUploadSuccess, bucketName)
}

func (r *Registry) IncHitachiGetStreamFail(bucketName, objectName string) {
	r.incByBucketObject(&r.mu, r.hitachiGetStreamFail, bucketName, objectName)
}

func (r *Registry) IncHitachiGetStreamSuccess(bucketName string) {
	r.incByBucket(&r.mu, r.hitachiGetStreamSuccess, bucketName)
}

func (r *Registry) IncFileUploadSuccess(bucketName string) {
	r.incByBucket(&r.mu, r.fileUploadSuccess, bucketName)
}

func (r *Registry) IncFileUploadFail(bucketName, objectName string) {
	r.incByBucketObject(&r.mu, r.fileUploadFail, bucketName, objectName)
}

func (r *Registry) IncFileDownloadSuccess(bucketName string) {
	r.incByBucket(&r.mu, r.fileDownloadSuccess, bucketName)
}

func (r *Registry) IncFileDownloadFail(bucketName, objectName string) {
	r.incByBucketObject(&r.mu, r.fileDownloadFail, bucketName, objectName)
}

func (r *Registry) IncGetStatsSuccess(bucketName string) {
	r.incByBucket(&r.mu, r.getStatsSuccess, bucketName)
}

func (r *Registry) IncGetStatsFail(bucketName, objectName string) {
	r.incByBucketObject(&r.mu, r.getStatsFail, bucketName, objectName)
}

func (r *Registry) IncPDFImageSuccess(bucketName string) {
	r.incByBucket(&r.mu, r.getPDFImageSuccess, bucketName)
}

func (r *Registry) IncPDFImageFail(bucketName, objectName string) {
	r.incByBucketObject(&r.mu, r.getPDFImageFail, bucketName, objectName)
}

func (r *Registry) incByBucket(mu *sync.Mutex, counter map[string]int64, bucketName string) {
	mu.Lock()
	defer mu.Unlock()
	counter[bucketName]++
}

func (r *Registry) incByBucketObject(mu *sync.Mutex, counter map[string]int64, bucketName, objectName string) {
	mu.Lock()
	defer mu.Unlock()
	counter[key(bucketName, objectName)]++
}

func (r *Registry) Render() string {
	r.mu.Lock()
	defer r.mu.Unlock()

	var b strings.Builder

	r.renderRequestMetrics(&b)
	r.renderSimpleCounter(&b, "ping_success_count", "Number of successful pings", r.pingSuccess)
	r.renderSimpleCounter(&b, "ping_failure_count", "Number of failed pings", r.pingFailure)
	r.renderSimpleCounter(&b, "hitachi_connection_fail_count", "Number of S3 failed connection attempts", r.hitachiConnectionFail)

	r.renderBucketCounter(&b, "hitachi_upload_fail_count", "Number of S3 failed file upload attempts", r.hitachiUploadFail)
	r.renderBucketCounter(&b, "hitachi_upload_success_count", "Number of S3 successful file uploads", r.hitachiUploadSuccess)
	r.renderBucketObjectCounter(&b, "hitachi_connection_get_stream_count", "Number of S3 failed file downloads", r.hitachiGetStreamFail)
	r.renderBucketCounter(&b, "hitachi_connection_get_stream_success_count", "Number of S3 successful file downloads", r.hitachiGetStreamSuccess)
	r.renderBucketCounter(&b, "file_upload_success_count", "Number of successful file uploads", r.fileUploadSuccess)
	r.renderBucketObjectCounter(&b, "file_upload_fail_count", "Number of failed file uploads", r.fileUploadFail)
	r.renderBucketCounter(&b, "file_download_success_count", "Number of successful file downloads", r.fileDownloadSuccess)
	r.renderBucketObjectCounter(&b, "file_download_fail_count", "Number of failed file downloads", r.fileDownloadFail)
	r.renderBucketObjectCounter(&b, "get_stats_fail_count", "Number of failed file stats", r.getStatsFail)
	r.renderBucketCounter(&b, "get_stats_success_count", "Number of successful file stats", r.getStatsSuccess)
	r.renderBucketCounter(&b, "get_pdf_image_success_count", "Number of successful pdf to image conversion", r.getPDFImageSuccess)
	r.renderBucketObjectCounter(&b, "get_pdf_image_fail_count", "Number of failed pdf to image conversion", r.getPDFImageFail)

	return b.String()
}

func (r *Registry) renderRequestMetrics(b *strings.Builder) {
	b.WriteString("# HELP dms_connector_requests_total Total connector requests\n")
	b.WriteString("# TYPE dms_connector_requests_total counter\n")

	requestKeys := sortedKeys(r.requests)
	for _, k := range requestKeys {
		parts := strings.Split(k, "|")
		fmt.Fprintf(
			b,
			"dms_connector_requests_total{endpoint=%q,method=%q,status=%q} %d\n",
			parts[0],
			parts[1],
			parts[2],
			r.requests[k],
		)
	}

	b.WriteString("# HELP dms_connector_request_duration_seconds_sum Total observed request duration\n")
	b.WriteString("# TYPE dms_connector_request_duration_seconds_sum counter\n")

	durationKeys := sortedKeys(r.durations)
	for _, k := range durationKeys {
		parts := strings.Split(k, "|")
		fmt.Fprintf(
			b,
			"dms_connector_request_duration_seconds_sum{endpoint=%q,method=%q} %.6f\n",
			parts[0],
			parts[1],
			r.durations[k],
		)
	}
}

func (r *Registry) renderSimpleCounter(b *strings.Builder, name, help string, value int64) {
	fmt.Fprintf(b, "# HELP %s %s\n", name, help)
	fmt.Fprintf(b, "# TYPE %s counter\n", name)
	fmt.Fprintf(b, "%s %d\n", name, value)
}

func (r *Registry) renderBucketCounter(b *strings.Builder, name, help string, values map[string]int64) {
	fmt.Fprintf(b, "# HELP %s %s\n", name, help)
	fmt.Fprintf(b, "# TYPE %s counter\n", name)

	for _, bucketName := range sortedKeys(values) {
		fmt.Fprintf(b, "%s{bucket_name=%q} %d\n", name, bucketName, values[bucketName])
	}
}

func (r *Registry) renderBucketObjectCounter(b *strings.Builder, name, help string, values map[string]int64) {
	fmt.Fprintf(b, "# HELP %s %s\n", name, help)
	fmt.Fprintf(b, "# TYPE %s counter\n", name)

	for _, entryKey := range sortedKeys(values) {
		parts := strings.Split(entryKey, "|")
		fmt.Fprintf(
			b,
			"%s{bucket_name=%q,object_name=%q} %d\n",
			name,
			parts[0],
			parts[1],
			values[entryKey],
		)
	}
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
