package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"os"
	"path"
	"strings"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type Client struct {
	client *minio.Client
	bucket string
	prefix string
	cache  *downloadCache
}

type FileMeta struct {
	FileID      string            `json:"file_id"`
	TenantID    string            `json:"tenant_id"`
	StoragePath string            `json:"storage_path"`
	Size        int64             `json:"size"`
	ContentType string            `json:"content_type"`
	UploadedAt  time.Time         `json:"uploaded_at"`
	Tags        map[string]string `json:"tags,omitempty"`
}

type UploadResponse struct {
	Name string `json:"name"`
	Size int64  `json:"size"`
}

type DownloadResponse struct {
	Body          io.ReadCloser
	ContentLength int64
}

func (dr *DownloadResponse) Close() error {
	return dr.Body.Close()
}

func NewClient(endpoint, accessKey, secretKey, region, bucket, prefix string, usePathStyle bool, timeout time.Duration, cacheDir string, cacheMaxFiles int) (*Client, error) {
	cache, err := newDownloadCache(cacheDir, cacheMaxFiles)
	if err != nil {
		return nil, err
	}

	parsedEndpoint := endpoint
	secure := true
	bucketLookup := minio.BucketLookupDNS

	if strings.Contains(endpoint, "://") {
		u, err := url.Parse(endpoint)
		if err != nil {
			return nil, err
		}
		parsedEndpoint = u.Host
		secure = u.Scheme == "https"
	}

	if usePathStyle {
		bucketLookup = minio.BucketLookupPath
	}

	minioClient, err := minio.New(parsedEndpoint, &minio.Options{
		Creds:        credentials.NewStaticV4(accessKey, secretKey, ""),
		Region:       region,
		Secure:       secure,
		Transport:    &http.Transport{Proxy: http.ProxyFromEnvironment},
		BucketLookup: bucketLookup,
	})
	if err != nil {
		return nil, err
	}

	return &Client{
		client: minioClient,
		bucket: bucket,
		prefix: strings.Trim(strings.Trim(prefix, "/"), "/"),
		cache:  cache,
	}, nil
}

func (c *Client) Health(ctx context.Context) error {
	exists, err := c.client.BucketExists(ctx, c.bucket)
	if err != nil {
		return err
	}
	if !exists {
		return fmt.Errorf("bucket %s does not exist", c.bucket)
	}
	return nil
}

func (c *Client) objectKey(objectPath string) string {
	cleaned := strings.Trim(objectPath, "/")
	if c.prefix == "" {
		return cleaned
	}
	return path.Join(c.prefix, cleaned)
}

func (c *Client) UploadBytes(ctx context.Context, filerPath, fieldName, fileName string, payload []byte) (*UploadResponse, error) {
	objectKey := c.objectKey(filerPath)
	reader := bytes.NewReader(payload)
	contentType := mime.TypeByExtension(path.Ext(fileName))

	n, err := c.client.PutObject(ctx, c.bucket, objectKey, reader, int64(reader.Len()), minio.PutObjectOptions{ContentType: contentType})
	if err != nil {
		return nil, err
	}

	return &UploadResponse{Name: n.Key, Size: n.Size}, nil
}

func (c *Client) UploadFile(ctx context.Context, filerPath, fieldName, fileName, localPath string) (*UploadResponse, error) {
	objectKey := c.objectKey(filerPath)
	file, err := os.Open(localPath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	stat, err := file.Stat()
	if err != nil {
		return nil, err
	}

	contentType := mime.TypeByExtension(path.Ext(fileName))

	n, err := c.client.PutObject(ctx, c.bucket, objectKey, file, stat.Size(), minio.PutObjectOptions{ContentType: contentType})
	if err != nil {
		return nil, err
	}

	return &UploadResponse{Name: n.Key, Size: n.Size}, nil
}

func (c *Client) UploadJSON(ctx context.Context, filerPath string, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}

	objectKey := c.objectKey(filerPath)
	reader := bytes.NewReader(b)
	_, err = c.client.PutObject(ctx, c.bucket, objectKey, reader, int64(reader.Len()), minio.PutObjectOptions{ContentType: "application/json"})
	return err
}

func (c *Client) Delete(ctx context.Context, filerPath string) error {
	objectKey := c.objectKey(filerPath)
	return c.client.RemoveObject(ctx, c.bucket, objectKey, minio.RemoveObjectOptions{})
}

func (c *Client) Download(ctx context.Context, filerPath string) (*DownloadResponse, error) {
	objectKey := c.objectKey(filerPath)
	obj, err := c.client.GetObject(ctx, c.bucket, objectKey, minio.GetObjectOptions{})
	if err != nil {
		return nil, err
	}

	info, err := obj.Stat()
	if err != nil {
		obj.Close()
		return nil, err
	}

	return &DownloadResponse{Body: obj, ContentLength: info.Size}, nil
}

func (c *Client) DownloadToFile(ctx context.Context, filerPath, localPath string) error {
	if c.cache == nil {
		return c.downloadToFile(ctx, filerPath, localPath)
	}

	cachedPath, err := c.cache.GetOrCreate(ctx, filerPath, func(cachePath string) error {
		return c.downloadToFile(ctx, filerPath, cachePath)
	})
	if err != nil {
		return err
	}

	return copyFile(cachedPath, localPath)
}

func (c *Client) downloadToFile(ctx context.Context, filerPath, localPath string) error {
	dr, err := c.Download(ctx, filerPath)
	if err != nil {
		return err
	}
	defer dr.Body.Close()

	f, err := os.Create(localPath)
	if err != nil {
		return err
	}
	defer f.Close()

	_, err = io.Copy(f, dr.Body)
	return err
}

func (c *Client) ReadJSON(ctx context.Context, filerPath string, out any) error {
	dr, err := c.Download(ctx, filerPath)
	if err != nil {
		return err
	}
	defer dr.Body.Close()

	b, err := io.ReadAll(dr.Body)
	if err != nil {
		return err
	}

	return json.Unmarshal(b, out)
}

func (c *Client) Exists(ctx context.Context, filerPath string) (bool, error) {
	objectKey := c.objectKey(filerPath)
	_, err := c.client.StatObject(ctx, c.bucket, objectKey, minio.StatObjectOptions{})
	if err != nil {
		respErr, ok := err.(minio.ErrorResponse)
		if ok && respErr.StatusCode == http.StatusNotFound {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func copyFile(srcPath, dstPath string) error {
	src, err := os.Open(srcPath)
	if err != nil {
		return err
	}
	defer src.Close()

	dst, err := os.Create(dstPath)
	if err != nil {
		return err
	}
	defer dst.Close()

	_, err = io.Copy(dst, src)
	return err
}
