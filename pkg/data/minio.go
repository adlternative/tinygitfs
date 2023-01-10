package data

import (
	"bytes"
	"errors"
	"fmt"
	log "github.com/sirupsen/logrus"
	"io"
	"io/ioutil"
	"net/url"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

var UserAgent = "GitFS"
var errNotSupported = errors.New("not supported")

type Object interface {
	Key() string
	Size() int64
	Mtime() time.Time
	IsDir() bool
}

type obj struct {
	key   string
	size  int64
	mtime time.Time
	isDir bool
}

func (o *obj) Key() string      { return o.key }
func (o *obj) Size() int64      { return o.size }
func (o *obj) Mtime() time.Time { return o.mtime }
func (o *obj) IsDir() bool      { return o.isDir }

var disableSha256Func = func(r *request.Request) {

	if op := r.Operation.Name; r.ClientInfo.ServiceID != "S3" || !(op == "PutObject" || op == "UploadPart") {
		return
	}
	if len(r.HTTPRequest.Header.Get("X-Amz-Content-Sha256")) != 0 {
		return
	}
	r.HTTPRequest.Header.Set("X-Amz-Content-Sha256", "UNSIGNED-PAYLOAD")
}

type MinioData struct {
	bucket string
	s3     *s3.S3
	ses    *session.Session
}

const awsDefaultRegion = "us-east-1"

func NewMinioData(dataOption *Option) (*MinioData, error) {
	uri, err := url.ParseRequestURI(dataOption.EndPoint)
	if err != nil {
		return nil, fmt.Errorf("invalid endpoint %s: %s", dataOption.EndPoint, err)
	}
	awsConfig := &aws.Config{
		Region:           aws.String(awsDefaultRegion),
		Endpoint:         &uri.Host,
		DisableSSL:       aws.Bool(strings.ToLower(uri.Scheme) != "https"),
		S3ForcePathStyle: aws.Bool(true),
	}
	if dataOption.Accesskey != "" {
		awsConfig.Credentials = credentials.NewStaticCredentials(dataOption.Accesskey, dataOption.SecretKey, "")
	}

	ses, err := session.NewSession(awsConfig)
	if err != nil {
		return nil, err
	}

	return &MinioData{
		bucket: dataOption.Bucket,
		s3:     s3.New(ses),
		ses:    ses,
	}, nil
}

func (s *MinioData) String() string {
	return fmt.Sprintf("s3://%s/", s.bucket)
}

func isExists(err error) bool {
	msg := err.Error()
	return strings.Contains(msg, s3.ErrCodeBucketAlreadyExists) || strings.Contains(msg, s3.ErrCodeBucketAlreadyOwnedByYou)
}

func (s *MinioData) Create() error {
	if _, err := s.List("", "", 1); err == nil {
		return nil
	}
	_, err := s.s3.CreateBucket(&s3.CreateBucketInput{Bucket: &s.bucket})
	if err != nil && isExists(err) {
		err = nil
	}
	return err
}

func (s *MinioData) Head(key string) (Object, error) {
	param := s3.HeadObjectInput{
		Bucket: &s.bucket,
		Key:    &key,
	}
	r, err := s.s3.HeadObject(&param)
	if err != nil {
		return nil, err
	}
	return &obj{
		key,
		*r.ContentLength,
		*r.LastModified,
		strings.HasSuffix(key, "/"),
	}, nil
}

func (s *MinioData) Get(key string, off, limit int64) (io.ReadCloser, error) {
	log.WithFields(log.Fields{
		"key":   key,
		"off":   off,
		"limit": limit,
	}).Debug("Minio Get")

	params := &s3.GetObjectInput{Bucket: &s.bucket, Key: &key}
	if off > 0 || limit > 0 {
		var r string
		if limit > 0 {
			r = fmt.Sprintf("bytes=%d-%d", off, off+limit-1)
		} else {
			r = fmt.Sprintf("bytes=%d-", off)
		}
		params.Range = &r
	}
	resp, err := s.s3.GetObject(params)
	if err != nil {
		return nil, err
	}
	if off == 0 && limit == -1 {
		cs := resp.Metadata[checksumAlgr]
		if cs != nil {
			resp.Body = verifyChecksum(resp.Body, *cs)
		}
	}
	return resp.Body, nil
}

func (s *MinioData) Put(key string, in io.Reader) error {
	log.WithField("key", key).Debug("Minio Put")

	var body io.ReadSeeker
	if b, ok := in.(io.ReadSeeker); ok {
		body = b
	} else {
		data, err := ioutil.ReadAll(in)
		if err != nil {
			return err
		}
		body = bytes.NewReader(data)
	}
	checksum := generateChecksum(body)
	params := &s3.PutObjectInput{
		Bucket:   &s.bucket,
		Key:      &key,
		Body:     body,
		Metadata: map[string]*string{checksumAlgr: &checksum},
	}
	_, err := s.s3.PutObject(params)
	return err
}

func (s *MinioData) Copy(dst, src string) error {
	src = s.bucket + "/" + src
	params := &s3.CopyObjectInput{
		Bucket:     &s.bucket,
		Key:        &dst,
		CopySource: &src,
	}
	_, err := s.s3.CopyObject(params)
	return err
}

func (s *MinioData) Delete(key string) error {
	param := s3.DeleteObjectInput{
		Bucket: &s.bucket,
		Key:    &key,
	}
	_, err := s.s3.DeleteObject(&param)
	return err
}

func (s *MinioData) List(prefix, marker string, limit int64) ([]Object, error) {
	param := s3.ListObjectsInput{
		Bucket:  &s.bucket,
		Prefix:  &prefix,
		Marker:  &marker,
		MaxKeys: &limit,
	}
	resp, err := s.s3.ListObjects(&param)
	if err != nil {
		return nil, err
	}
	n := len(resp.Contents)
	objs := make([]Object, n)
	for i := 0; i < n; i++ {
		o := resp.Contents[i]
		objs[i] = &obj{
			*o.Key,
			*o.Size,
			*o.LastModified,
			strings.HasSuffix(*o.Key, "/"),
		}
	}
	return objs, nil
}

func (s *MinioData) ListAll(prefix, marker string) (<-chan Object, error) {
	return nil, errNotSupported
}

type MultipartUpload struct {
	MinPartSize int
	MaxCount    int
	UploadID    string
}

type Part struct {
	Num  int
	Size int
	ETag string
}

type PendingPart struct {
	Key      string
	UploadID string
	Created  time.Time
}

func (s *MinioData) CreateMultipartUpload(key string) (*MultipartUpload, error) {
	params := &s3.CreateMultipartUploadInput{
		Bucket: &s.bucket,
		Key:    &key,
	}
	resp, err := s.s3.CreateMultipartUpload(params)
	if err != nil {
		return nil, err
	}

	return &MultipartUpload{UploadID: *resp.UploadId, MinPartSize: 5 << 20, MaxCount: 10000}, nil
}

func (s *MinioData) UploadPart(key string, uploadID string, num int, body []byte) (*Part, error) {
	n := int64(num)
	params := &s3.UploadPartInput{
		Bucket:     &s.bucket,
		Key:        &key,
		UploadId:   &uploadID,
		Body:       bytes.NewReader(body),
		PartNumber: &n,
	}
	resp, err := s.s3.UploadPart(params)
	if err != nil {
		return nil, err
	}
	return &Part{Num: num, ETag: *resp.ETag}, nil
}

func (s *MinioData) AbortMultipartUpload(key string, uploadID string) {
	params := &s3.AbortMultipartUploadInput{
		Bucket:   &s.bucket,
		Key:      &key,
		UploadId: &uploadID,
	}
	_, _ = s.s3.AbortMultipartUpload(params)
}

func (s *MinioData) CompleteMultipartUpload(key string, uploadID string, parts []*Part) error {
	var s3Parts []*s3.CompletedPart
	for i := range parts {
		n := new(int64)
		*n = int64(parts[i].Num)
		s3Parts = append(s3Parts, &s3.CompletedPart{ETag: &parts[i].ETag, PartNumber: n})
	}
	params := &s3.CompleteMultipartUploadInput{
		Bucket:          &s.bucket,
		Key:             &key,
		UploadId:        &uploadID,
		MultipartUpload: &s3.CompletedMultipartUpload{Parts: s3Parts},
	}
	_, err := s.s3.CompleteMultipartUpload(params)
	return err
}

func (s *MinioData) ListMultipartUploads(marker string) ([]*PendingPart, string, error) {
	input := &s3.ListMultipartUploadsInput{
		Bucket:    aws.String(s.bucket),
		KeyMarker: aws.String(marker),
	}

	result, err := s.s3.ListMultipartUploads(input)
	if err != nil {
		return nil, "", err
	}
	parts := make([]*PendingPart, len(result.Uploads))
	for i, u := range result.Uploads {
		parts[i] = &PendingPart{*u.Key, *u.UploadId, *u.Initiated}
	}
	var nextMarker string
	if result.NextKeyMarker != nil {
		nextMarker = *result.NextKeyMarker
	}
	return parts, nextMarker, nil
}

func (s *MinioData) Init() error {
	return s.Create()
}
