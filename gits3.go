package gits3

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
)

var (
	Name     string
	Version  string
	Revision string
)

type storageURL struct {
	Bucket string
	Prefix string
}

type snapshot struct {
	Key       string    `json:"key"`
	CreatedAt time.Time `json:"created_at"`
	Commit    string    `json:"commit"`
	Head      string    `json:"head,omitempty"`
}

type remoteHelper struct {
	name    string
	rawURL  string
	target  storageURL
	client  *s3.Client
	dryRun  bool
	force   bool
	verbose bool

	cachedBundleFile string
	cachedSnap       *snapshot
	cachedETag       string
}

type pushCommand struct {
	Force bool
	Src   string
	Dst   string
}

type refInfo struct {
	Name string
	Hash string
}

func Main() {
	log.SetFlags(0)

	if len(os.Args) >= 2 && isControlCommand(os.Args[1]) {
		if err := runControlCommand(os.Args[1:]); err != nil {
			log.Fatalln(err)
		}
		return
	}

	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}

	name := os.Args[1]
	rawURL := name
	if len(os.Args) >= 3 {
		rawURL = os.Args[2]
	}

	if err := runRemoteHelper(name, rawURL); err != nil {
		log.Fatalln(err)
	}
}

func usage() {
	fmt.Fprintf(os.Stderr, `git-remote-s3 is a Git remote helper backed by S3 bundle snapshots.

Usage:
  git clone s3://bucket/prefix
  git remote add backup s3://bucket/prefix
  git fetch backup
  git push backup main
  git-remote-s3 version
`)
}

func isControlCommand(arg string) bool {
	switch arg {
	case "version", "--version", "-v", "help", "--help", "-h":
		return true
	default:
		return false
	}
}

func runControlCommand(args []string) error {
	switch args[0] {
	case "version", "--version", "-v":
		fmt.Printf("%s %s (rev: %s/%s)\n", Name, Version, Revision, runtime.Version())
	case "help", "--help", "-h":
		usage()
	default:
		return fmt.Errorf("unknown command %q", args[0])
	}
	return nil
}

func runRemoteHelper(name, rawURL string) error {
	target, err := parseStorageURL(rawURL)
	if err != nil {
		return err
	}

	remoteName := sanitizeRemoteName(name)

	ctx := context.Background()
	configPrefix := "remote." + remoteName
	var pathStyle *bool
	if v := gitConfigValue(configPrefix + ".s3-path-style"); v != "" {
		b := v == "true"
		pathStyle = &b
	}
	client, err := newS3Client(ctx, s3ClientOptions{
		Profile:   gitConfigValue(configPrefix + ".s3-profile"),
		Endpoint:  gitConfigValue(configPrefix + ".s3-endpoint"),
		PathStyle: pathStyle,
	})
	if err != nil {
		return err
	}

	h := &remoteHelper{
		name:   remoteName,
		rawURL: rawURL,
		target: target,
		client: client,
	}
	return h.serve(ctx, os.Stdin, os.Stdout)
}

func (h *remoteHelper) clearCache() {
	if h.cachedBundleFile != "" {
		os.Remove(h.cachedBundleFile)
		h.cachedBundleFile = ""
	}
	h.cachedSnap = nil
	h.cachedETag = ""
}

func (h *remoteHelper) serve(ctx context.Context, r io.Reader, w io.Writer) error {
	defer h.clearCache()
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}

		switch {
		case line == "capabilities":
			fmt.Fprintln(w, "option")
			fmt.Fprintln(w, "fetch")
			fmt.Fprintln(w, "push")
			fmt.Fprintln(w)
		case line == "list":
			if err := h.handleList(ctx, w); err != nil {
				return err
			}
		case line == "list for-push":
			if err := h.handleList(ctx, w); err != nil {
				return err
			}
		case strings.HasPrefix(line, "option "):
			h.handleOption(w, strings.TrimPrefix(line, "option "))
		case strings.HasPrefix(line, "fetch "):
			lines, err := readBatch(scanner, line)
			if err != nil {
				return err
			}
			if err := h.handleFetch(ctx, w, lines); err != nil {
				return err
			}
		case strings.HasPrefix(line, "push "):
			lines, err := readBatch(scanner, line)
			if err != nil {
				return err
			}
			if err := h.handlePush(ctx, w, lines); err != nil {
				return err
			}
		default:
			fmt.Fprintln(w)
		}
	}

	return scanner.Err()
}

func readBatch(scanner *bufio.Scanner, first string) ([]string, error) {
	lines := []string{first}
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			break
		}
		if strings.HasPrefix(line, "option ") {
			continue
		}
		lines = append(lines, line)
	}
	return lines, scanner.Err()
}

func (h *remoteHelper) handleOption(w io.Writer, payload string) {
	fields := strings.SplitN(payload, " ", 2)
	name := fields[0]
	value := ""
	if len(fields) == 2 {
		value = fields[1]
	}

	switch name {
	case "verbosity":
		h.verbose = value != "0"
		fmt.Fprintln(w, "ok")
	case "progress", "cloning", "followtags", "check-connectivity":
		fmt.Fprintln(w, "ok")
	case "dry-run":
		h.dryRun = value == "true"
		fmt.Fprintln(w, "ok")
	case "force":
		h.force = value == "true"
		fmt.Fprintln(w, "ok")
	default:
		fmt.Fprintln(w, "unsupported")
	}
}

func (h *remoteHelper) handleList(ctx context.Context, w io.Writer) error {
	h.clearCache()

	snap, etag, err := readLatest(ctx, h.client, h.target)
	if err != nil {
		if isNotFound(err) {
			fmt.Fprintln(w)
			return nil
		}
		return err
	}

	bundleFile, err := downloadTempFile(ctx, h.client, h.target.Bucket, snap.Key)
	if err != nil {
		return err
	}

	refs, err := bundleRefs(bundleFile)
	if err != nil {
		os.Remove(bundleFile)
		return err
	}

	h.cachedBundleFile = bundleFile
	h.cachedSnap = &snap
	h.cachedETag = etag

	if snap.Head != "" {
		fmt.Fprintf(w, "@%s HEAD\n", snap.Head)
	}
	for _, ref := range refs {
		fmt.Fprintf(w, "%s %s\n", ref.Hash, ref.Name)
	}
	fmt.Fprintln(w)
	return nil
}

func (h *remoteHelper) handleFetch(ctx context.Context, w io.Writer, lines []string) error {
	bundleFile := h.cachedBundleFile
	if bundleFile == "" {
		snap, _, err := readLatest(ctx, h.client, h.target)
		if err != nil {
			if isNotFound(err) {
				fmt.Fprintln(w)
				return nil
			}
			return err
		}

		bundleFile, err = downloadTempFile(ctx, h.client, h.target.Bucket, snap.Key)
		if err != nil {
			return err
		}
		defer os.Remove(bundleFile)
	}

	args := []string{"fetch", "--quiet", bundleFile}
	for _, line := range lines {
		fields := strings.SplitN(line, " ", 3)
		if len(fields) < 3 {
			continue
		}
		args = append(args, fields[2])
	}

	if err := runGitWithEnv("", nil, args...); err != nil {
		return err
	}
	fmt.Fprintln(w)
	return nil
}

func (h *remoteHelper) handlePush(ctx context.Context, w io.Writer, lines []string) error {
	pushes, err := parsePushBatch(lines)
	if err != nil {
		return err
	}

	gitDir, err := gitOutput("", "rev-parse", "--absolute-git-dir")
	if err != nil {
		return fmt.Errorf("resolve local git dir: %w", err)
	}

	tmpDir, err := os.MkdirTemp("", "git-remote-s3-*.git")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tmpDir)

	if err := runGitWithEnv("", nil, "init", "--bare", tmpDir); err != nil {
		return err
	}

	var etag string
	if h.cachedSnap != nil {
		etag = h.cachedETag
		bundleFile := h.cachedBundleFile
		if bundleFile != "" {
			if err := runGitWithEnv("", nil, "--git-dir", tmpDir, "fetch", "--quiet", bundleFile, "refs/*:refs/*"); err != nil {
				return err
			}
		}
	} else {
		snap, e, err := readLatest(ctx, h.client, h.target)
		if err == nil {
			etag = e
			bundleFile, derr := downloadTempFile(ctx, h.client, h.target.Bucket, snap.Key)
			if derr != nil {
				return derr
			}
			defer os.Remove(bundleFile)
			if err := runGitWithEnv("", nil, "--git-dir", tmpDir, "fetch", "--quiet", bundleFile, "refs/*:refs/*"); err != nil {
				return err
			}
		} else if !isNotFound(err) {
			return err
		}
	}

	for _, cmd := range pushes {
		if cmd.Src == "" {
			if err := runGitWithEnv("", nil, "--git-dir", tmpDir, "update-ref", "-d", cmd.Dst); err != nil {
				fmt.Fprintf(w, "error %s delete failed\n", cmd.Dst)
				fmt.Fprintln(w)
				return nil
			}
			continue
		}

		refspec := cmd.Src + ":" + cmd.Dst
		if cmd.Force || h.force {
			refspec = "+" + refspec
		}
		if err := runGitWithEnv("", nil, "--git-dir", tmpDir, "fetch", "--quiet", gitDir, refspec); err != nil {
			fmt.Fprintf(w, "error %s push failed\n", cmd.Dst)
			fmt.Fprintln(w)
			return nil
		}
	}

	if h.dryRun {
		for _, cmd := range pushes {
			fmt.Fprintf(w, "ok %s\n", cmd.Dst)
		}
		fmt.Fprintln(w)
		return nil
	}

	refs, err := bareRefs(tmpDir)
	if err != nil {
		return err
	}
	if len(refs) == 0 {
		for _, cmd := range pushes {
			fmt.Fprintf(w, "error %s empty remote is not supported\n", cmd.Dst)
		}
		fmt.Fprintln(w)
		return nil
	}

	headRef, _ := gitOutput("", "--git-dir", tmpDir, "symbolic-ref", "HEAD")
	commit, err := gitOutput("", "--git-dir", tmpDir, "rev-parse", "HEAD")
	if err != nil && headRef != "" {
		commit, err = gitOutput("", "--git-dir", tmpDir, "rev-parse", headRef)
	}
	if err != nil {
		commit = refs[0].Hash
	}

	tmpBundle, err := os.CreateTemp("", "git-remote-s3-*.bundle")
	if err != nil {
		return err
	}
	tmpBundleName := tmpBundle.Name()
	tmpBundle.Close()
	defer os.Remove(tmpBundleName)

	if err := runGitWithEnv("", nil, "--git-dir", tmpDir, "bundle", "create", tmpBundleName, "--all"); err != nil {
		return err
	}

	createdAt := time.Now().UTC().Truncate(time.Second)
	newSnap := snapshot{
		Key:       joinKey(h.target.Prefix, "snapshots", createdAt.Format("20060102T150405Z")+"-"+shortCommit(commit)+".bundle"),
		CreatedAt: createdAt,
		Commit:    commit,
		Head:      headRef,
	}
	if err := uploadFile(ctx, h.client, h.target.Bucket, newSnap.Key, tmpBundleName); err != nil {
		return err
	}
	if err := writeLatest(ctx, h.client, h.target, newSnap, etag); err != nil {
		return err
	}

	for _, cmd := range pushes {
		fmt.Fprintf(w, "ok %s\n", cmd.Dst)
	}
	fmt.Fprintln(w)
	return nil
}

func parsePushBatch(lines []string) ([]pushCommand, error) {
	var pushes []pushCommand
	for _, line := range lines {
		if !strings.HasPrefix(line, "push ") {
			continue
		}
		spec := strings.TrimPrefix(line, "push ")
		force := strings.HasPrefix(spec, "+")
		spec = strings.TrimPrefix(spec, "+")
		parts := strings.SplitN(spec, ":", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid push command %q", line)
		}
		pushes = append(pushes, pushCommand{
			Force: force,
			Src:   parts[0],
			Dst:   parts[1],
		})
	}
	return pushes, nil
}

type s3ClientOptions struct {
	Profile      string
	Endpoint     string
	PathStyle    *bool // nil = auto, true/false = explicit
}

func newS3Client(ctx context.Context, opts s3ClientOptions) (*s3.Client, error) {
	var cfgOpts []func(*config.LoadOptions) error
	if opts.Profile != "" {
		cfgOpts = append(cfgOpts, config.WithSharedConfigProfile(opts.Profile))
	}
	cfg, err := config.LoadDefaultConfig(ctx, cfgOpts...)
	if err != nil {
		return nil, fmt.Errorf("load AWS config: %w", err)
	}
	var s3Opts []func(*s3.Options)
	endpoint := opts.Endpoint
	if endpoint == "" && cfg.BaseEndpoint == nil {
		// Workaround: Go SDK v2 does not merge endpoint_url from
		// credentials file into the shared config. Read it ourselves.
		endpoint = readProfileEndpointURL(opts.Profile)
	}
	if endpoint != "" {
		s3Opts = append(s3Opts, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(endpoint)
			// Disable automatic checksum to avoid aws-chunked transfer
			// encoding and response validation warnings on S3-compatible
			// services (e.g. OCI Object Storage).
			o.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
			o.ResponseChecksumValidation = aws.ResponseChecksumValidationWhenRequired
		})
	}
	usePathStyle := false
	if opts.PathStyle != nil {
		usePathStyle = *opts.PathStyle
	} else if endpoint != "" {
		// Default to path-style for custom endpoints (S3-compatible services).
		usePathStyle = true
	} else if style := readProfileAddressingStyle(opts.Profile); style == "path" {
		// Respect addressing_style=path in AWS CLI config.
		usePathStyle = true
	}
	if usePathStyle {
		s3Opts = append(s3Opts, func(o *s3.Options) {
			o.UsePathStyle = true
		})
	}
	return s3.NewFromConfig(cfg, s3Opts...), nil
}

// readProfileEndpointURL reads endpoint_url from ~/.aws/credentials or
// ~/.aws/config for the given profile. This works around the Go SDK v2
// not merging endpoint_url from the credentials file during config loading.
func readProfileEndpointURL(profile string) string {
	if profile == "" {
		profile = os.Getenv("AWS_PROFILE")
	}
	if profile == "" {
		profile = "default"
	}
	for _, path := range awsConfigPaths() {
		if v := readINIKey(path, profile, "endpoint_url"); v != "" {
			return v
		}
	}
	return ""
}

// readProfileAddressingStyle reads the s3 addressing_style from
// ~/.aws/config or ~/.aws/credentials for the given profile.
func readProfileAddressingStyle(profile string) string {
	if profile == "" {
		profile = os.Getenv("AWS_PROFILE")
	}
	if profile == "" {
		profile = "default"
	}
	for _, path := range awsConfigPaths() {
		if v := readINIKey(path, profile, "addressing_style"); v != "" {
			return v
		}
	}
	return ""
}

func awsConfigPaths() []string {
	var paths []string
	if v := os.Getenv("AWS_SHARED_CREDENTIALS_FILE"); v != "" {
		paths = append(paths, v)
	} else {
		if home, err := os.UserHomeDir(); err == nil {
			paths = append(paths, filepath.Join(home, ".aws", "credentials"))
		}
	}
	if v := os.Getenv("AWS_CONFIG_FILE"); v != "" {
		paths = append(paths, v)
	} else {
		if home, err := os.UserHomeDir(); err == nil {
			paths = append(paths, filepath.Join(home, ".aws", "config"))
		}
	}
	return paths
}

func readINIKey(path, profile, key string) string {
	f, err := os.Open(path)
	if err != nil {
		return ""
	}
	defer f.Close()

	inProfile := false
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || line[0] == '#' || line[0] == ';' {
			continue
		}
		if line[0] == '[' {
			section := strings.Trim(line, "[]")
			section = strings.TrimSpace(section)
			section = strings.TrimPrefix(section, "profile ")
			inProfile = section == profile
			continue
		}
		if inProfile {
			parts := strings.SplitN(line, "=", 2)
			if len(parts) == 2 && strings.TrimSpace(parts[0]) == key {
				return strings.TrimSpace(parts[1])
			}
		}
	}
	return ""
}

func gitConfigValue(key string) string {
	out, err := exec.Command("git", "config", "--default", "", key).Output()
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(out))
}

func parseStorageURL(raw string) (storageURL, error) {
	if !strings.HasPrefix(raw, "s3://") {
		return storageURL{}, fmt.Errorf("invalid S3 URL %q", raw)
	}
	trimmed := strings.TrimPrefix(raw, "s3://")
	parts := strings.SplitN(trimmed, "/", 2)
	if parts[0] == "" {
		return storageURL{}, fmt.Errorf("missing bucket in %q", raw)
	}
	prefix := ""
	if len(parts) == 2 {
		prefix = strings.Trim(parts[1], "/")
	}
	return storageURL{Bucket: parts[0], Prefix: prefix}, nil
}

func sanitizeRemoteName(name string) string {
	name = filepath.Base(name)
	if name == "." || name == "/" || name == "" || strings.Contains(name, "://") {
		return "origin"
	}
	return strings.ReplaceAll(name, "/", "-")
}

func gitOutput(dir string, args ...string) (string, error) {
	cmd := exec.Command("git", prependDir(dir, args)...)
	out, err := cmd.Output()
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(out)), nil
}

func runGitWithEnv(dir string, env []string, args ...string) error {
	cmd := exec.Command("git", prependDir(dir, args)...)
	cmd.Env = append(os.Environ(), env...)
	cmd.Stdout = io.Discard
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

func prependDir(dir string, args []string) []string {
	if dir == "" {
		return args
	}
	return append([]string{"-C", dir}, args...)
}

func uploadFile(ctx context.Context, client *s3.Client, bucket, key, filename string) error {
	f, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		return err
	}

	_, err = client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(bucket),
		Key:           aws.String(key),
		Body:          f,
		ContentLength: aws.Int64(fi.Size()),
	})
	if err != nil {
		return fmt.Errorf("upload s3://%s/%s: %w", bucket, key, err)
	}
	return nil
}

func writeLatest(ctx context.Context, client *s3.Client, target storageURL, snap snapshot, etag string) error {
	b, err := json.MarshalIndent(snap, "", "  ")
	if err != nil {
		return err
	}
	input := &s3.PutObjectInput{
		Bucket:        aws.String(target.Bucket),
		Key:           aws.String(joinKey(target.Prefix, "latest.json")),
		Body:          bytes.NewReader(b),
		ContentLength: aws.Int64(int64(len(b))),
		ContentType:   aws.String("application/json"),
	}
	if etag != "" {
		input.IfMatch = aws.String(etag)
	} else {
		input.IfNoneMatch = aws.String("*")
	}
	_, err = client.PutObject(ctx, input)
	if err != nil {
		if isPreconditionFailed(err) {
			return fmt.Errorf("concurrent push detected: remote was updated since last read, please retry")
		}
		return fmt.Errorf("write latest pointer: %w", err)
	}
	return nil
}

func readLatest(ctx context.Context, client *s3.Client, target storageURL) (snapshot, string, error) {
	out, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(target.Bucket),
		Key:    aws.String(joinKey(target.Prefix, "latest.json")),
	})
	if err != nil {
		return snapshot{}, "", fmt.Errorf("read latest pointer: %w", err)
	}
	defer out.Body.Close()

	etag := ""
	if out.ETag != nil {
		etag = *out.ETag
	}

	var snap snapshot
	if err := json.NewDecoder(out.Body).Decode(&snap); err != nil {
		return snapshot{}, "", fmt.Errorf("decode latest pointer: %w", err)
	}
	if snap.Key == "" {
		return snapshot{}, "", errors.New("latest pointer is missing snapshot key")
	}
	return snap, etag, nil
}

func downloadTempFile(ctx context.Context, client *s3.Client, bucket, key string) (string, error) {
	out, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return "", fmt.Errorf("download s3://%s/%s: %w", bucket, key, err)
	}
	defer out.Body.Close()

	f, err := os.CreateTemp("", "git-remote-s3-*.bundle")
	if err != nil {
		return "", err
	}
	defer f.Close()

	if _, err := io.Copy(f, out.Body); err != nil {
		os.Remove(f.Name())
		return "", err
	}
	return f.Name(), nil
}

func bundleRefs(bundleFile string) ([]refInfo, error) {
	cmd := exec.Command("git", "bundle", "list-heads", bundleFile)
	out, err := cmd.Output()
	if err != nil {
		return nil, err
	}

	var refs []refInfo
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		if fields[1] == "HEAD" {
			continue
		}
		refs = append(refs, refInfo{Name: fields[1], Hash: fields[0]})
	}
	sort.Slice(refs, func(i, j int) bool {
		return refs[i].Name < refs[j].Name
	})
	return refs, nil
}

func bareRefs(gitDir string) ([]refInfo, error) {
	cmd := exec.Command("git", "--git-dir", gitDir, "for-each-ref", "--format=%(objectname) %(refname)", "refs/heads", "refs/tags")
	out, err := cmd.Output()
	if err != nil {
		return nil, err
	}

	var refs []refInfo
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		fields := strings.Fields(line)
		if len(fields) != 2 {
			continue
		}
		refs = append(refs, refInfo{Name: fields[1], Hash: fields[0]})
	}
	sort.Slice(refs, func(i, j int) bool {
		return refs[i].Name < refs[j].Name
	})
	return refs, nil
}

func isNotFound(err error) bool {
	if err == nil {
		return false
	}
	var nsk *s3types.NoSuchKey
	if errors.As(err, &nsk) {
		return true
	}
	var nf *s3types.NotFound
	return errors.As(err, &nf)
}

func isPreconditionFailed(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "PreconditionFailed") || strings.Contains(err.Error(), "412")
}

func shortCommit(commit string) string {
	if len(commit) > 12 {
		return commit[:12]
	}
	return commit
}

func joinKey(parts ...string) string {
	var filtered []string
	for _, part := range parts {
		part = strings.Trim(part, "/")
		if part != "" {
			filtered = append(filtered, part)
		}
	}
	return strings.Join(filtered, "/")
}
