package gits3

import "testing"

func TestParseStorageURL(t *testing.T) {
	t.Parallel()

	tests := []struct {
		raw    string
		bucket string
		prefix string
		ok     bool
	}{
		{raw: "s3://bucket", bucket: "bucket", prefix: "", ok: true},
		{raw: "s3://bucket/path/to/repo", bucket: "bucket", prefix: "path/to/repo", ok: true},
		{raw: "s3://bucket/path/to/repo/", bucket: "bucket", prefix: "path/to/repo", ok: true},
		{raw: "https://bucket/path", ok: false},
		{raw: "s3:///path", ok: false},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.raw, func(t *testing.T) {
			t.Parallel()
			got, err := parseStorageURL(tt.raw)
			if tt.ok && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !tt.ok {
				if err == nil {
					t.Fatal("expected error")
				}
				return
			}
			if got.Bucket != tt.bucket || got.Prefix != tt.prefix {
				t.Fatalf("got %+v", got)
			}
		})
	}
}

func TestJoinKey(t *testing.T) {
	t.Parallel()

	if got := joinKey("/a/", "/b/", "c/"); got != "a/b/c" {
		t.Fatalf("got %q", got)
	}
}

func TestSanitizeRemoteName(t *testing.T) {
	t.Parallel()

	tests := map[string]string{
		"origin":              "origin",
		"s3://bucket/repo":    "repo",
		"/tmp/work/backup":    "backup",
		"team/archive/backup": "backup",
	}

	for input, want := range tests {
		if got := sanitizeRemoteName(input); got != want {
			t.Fatalf("%q => %q, want %q", input, got, want)
		}
	}
}

func TestParsePushBatch(t *testing.T) {
	t.Parallel()

	got, err := parsePushBatch([]string{
		"push +refs/heads/main:refs/heads/main",
		"push :refs/heads/old",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("got %d pushes", len(got))
	}
	if !got[0].Force || got[0].Src != "refs/heads/main" || got[0].Dst != "refs/heads/main" {
		t.Fatalf("got %#v", got[0])
	}
	if got[1].Src != "" || got[1].Dst != "refs/heads/old" {
		t.Fatalf("got %#v", got[1])
	}
}
