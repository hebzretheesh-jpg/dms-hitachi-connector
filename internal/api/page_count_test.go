package api

import (
	"testing"

	"github.com/mygithub/dms-hitachi-connector/internal/storage"
)

func TestPageCountMetadataTrusted(t *testing.T) {
	tests := []struct {
		name string
		tags storage.Tags
		want bool
	}{
		{
			name: "missing count",
			tags: storage.Tags{},
		},
		{
			name: "invalid count",
			tags: storage.Tags{"page_count": "unknown"},
		},
		{
			name: "legacy one-page metadata",
			tags: storage.Tags{
				"page_count":         "1",
				pageCountVerifiedTag: "true",
			},
		},
		{
			name: "one-page count re-read by this connector is trusted",
			tags: storage.Tags{
				"page_count":         "1",
				pageCountVerifiedTag: "true",
				pageCountRepairedTag: "true",
			},
			want: true,
		},
		{
			name: "multi-page metadata",
			tags: storage.Tags{"page_count": "12"},
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := pageCountMetadataTrusted(storage.FileMeta{Tags: tt.tags}); got != tt.want {
				t.Fatalf("pageCountMetadataTrusted() = %t, want %t", got, tt.want)
			}
		})
	}
}

func TestLocalFirstMetadataFileID(t *testing.T) {
	tests := []struct {
		name        string
		requestPath string
		wantFileID  string
		wantOK      bool
	}{
		{
			name:        "stats",
			requestPath: "/dss/api/stats/12345",
			wantFileID:  "12345",
			wantOK:      true,
		},
		{
			name:        "preview page path",
			requestPath: "/dss/api/getimage/12345/2",
			wantFileID:  "12345",
			wantOK:      true,
		},
		{
			name:        "unrelated retrieval",
			requestPath: "/dss/api/get/12345",
		},
		{
			name:        "empty file id",
			requestPath: "/dss/api/stats/",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotFileID, gotOK := localFirstMetadataFileID(tt.requestPath)
			if gotFileID != tt.wantFileID || gotOK != tt.wantOK {
				t.Fatalf("localFirstMetadataFileID(%q) = (%q, %t), want (%q, %t)",
					tt.requestPath, gotFileID, gotOK, tt.wantFileID, tt.wantOK)
			}
		})
	}
}
