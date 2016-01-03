// Package gcs provides a storagedriver.StorageDriver implementation to
// store blobs in Google cloud storage.
//
// This package leverages the google.golang.org/cloud/storage client library
//for interfacing with gcs.
//
// Because gcs is a key, value store the Stat call does not support last modification
// time for directories (directories are an abstraction for key, value stores)
//
// Note that the contents of incomplete uploads are not accessible even though
// Stat returns their length
//
// +build include_gcs

package gcs

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"golang.org/x/net/context"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"

	"google.golang.org/api/googleapi"
	"google.golang.org/cloud"
	"google.golang.org/cloud/storage"

	ctx "github.com/docker/distribution/context"
	storagedriver "github.com/docker/distribution/registry/storage/driver"
	"github.com/docker/distribution/registry/storage/driver/base"
	"github.com/docker/distribution/registry/storage/driver/factory"
)

const driverName = "gcs"
const dummyProjectID = "<unknown>"

const minChunkSize = 256 * 1024
const maxChunkSize = 20 * minChunkSize

var rangeHeader = regexp.MustCompile(`^bytes=([0-9])+-([0-9]+)$`)

type uploadSession struct {
	name       string
	sessionURI string
	buffer     []byte
	offset     int64
}

//DriverParameters A struct that encapsulates all of the driver parameters after all values have been set
type driverParameters struct {
	bucket        string
	keyfile       string
	rootDirectory string
}

func init() {
	factory.Register(driverName, &gcsDriverFactory{})
}

// gcsDriverFactory implements the factory.StorageDriverFactory interface
type gcsDriverFactory struct{}

// Create StorageDriver from parameters
func (factory *gcsDriverFactory) Create(parameters map[string]interface{}) (storagedriver.StorageDriver, error) {
	return FromParameters(parameters)
}

// driver is a storagedriver.StorageDriver implementation backed by GCS
// Objects are stored at absolute keys in the provided bucket.
type driver struct {
	client        *http.Client
	bucket        string
	email         string
	privateKey    []byte
	rootDirectory string
	sessions      map[string]*uploadSession
}

// FromParameters constructs a new Driver with a given parameters map
// Required parameters:
// - bucket
func FromParameters(parameters map[string]interface{}) (storagedriver.StorageDriver, error) {

	bucket, ok := parameters["bucket"]
	if !ok || fmt.Sprint(bucket) == "" {
		return nil, fmt.Errorf("No bucket parameter provided")
	}

	keyfile, ok := parameters["keyfile"]
	if !ok {
		keyfile = ""
	}

	rootDirectory, ok := parameters["rootdirectory"]
	if !ok {
		rootDirectory = ""
	}
	params := driverParameters{
		fmt.Sprint(bucket),
		fmt.Sprint(keyfile),
		fmt.Sprint(rootDirectory),
	}

	return New(params)
}

// New constructs a new driver
func New(params driverParameters) (storagedriver.StorageDriver, error) {
	var ts oauth2.TokenSource
	var err error
	rootDirectory := strings.Trim(params.rootDirectory, "/")
	if rootDirectory != "" {
		rootDirectory += "/"
	}
	d := &driver{
		bucket:        params.bucket,
		rootDirectory: rootDirectory,
		sessions:      make(map[string]*uploadSession),
	}
	if params.keyfile == "" {
		ts, err = google.DefaultTokenSource(context.Background(), storage.ScopeFullControl)
		if err != nil {
			return nil, err
		}
	} else {
		jsonKey, err := ioutil.ReadFile(params.keyfile)
		if err != nil {
			return nil, err
		}
		conf, err := google.JWTConfigFromJSON(
			jsonKey,
			storage.ScopeFullControl,
		)
		if err != nil {
			return nil, err
		}
		ts = conf.TokenSource(context.Background())
		d.email = conf.Email
		d.privateKey = conf.PrivateKey
	}
	client := oauth2.NewClient(context.Background(), ts)
	d.client = client
	if err != nil {
		return nil, err
	}
	return &base.Base{
		StorageDriver: d,
	}, nil
}

// Implement the storagedriver.StorageDriver interface

func (d *driver) Name() string {
	return driverName
}

// GetContent retrieves the content stored at "path" as a []byte.
// This should primarily be used for small objects.
func (d *driver) GetContent(context ctx.Context, path string) ([]byte, error) {
	rc, err := d.ReadStream(context, path, 0)
	if err != nil {
		return nil, err
	}
	defer rc.Close()

	p, err := ioutil.ReadAll(rc)
	if err != nil {
		return nil, err
	}
	return p, nil
}

// PutContent stores the []byte content at a location designated by "path".
// This should primarily be used for small objects.
func (d *driver) PutContent(context ctx.Context, path string, contents []byte) error {
	wc := storage.NewWriter(d.context(context), d.bucket, d.pathToKey(path))
	wc.ContentType = "application/octet-stream"
	defer wc.Close()
	_, err := wc.Write(contents)
	return err
}

// ReadStream retrieves an io.ReadCloser for the content stored at "path"
// with a given byte offset.
// May be used to resume reading a stream by providing a nonzero offset.
func (d *driver) ReadStream(context ctx.Context, path string, offset int64) (io.ReadCloser, error) {
	name := d.pathToKey(path)

	// copied from google.golang.org/cloud/storage#NewReader :
	// to set the additional "Range" header
	u := &url.URL{
		Scheme: "https",
		Host:   "storage.googleapis.com",
		Path:   fmt.Sprintf("/%s/%s", d.bucket, name),
	}
	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return nil, err
	}
	if offset > 0 {
		req.Header.Set("Range", fmt.Sprintf("bytes=%v-", offset))
	}
	res, err := d.client.Do(req)
	if err != nil {
		return nil, err
	}
	if res.StatusCode == http.StatusNotFound {
		res.Body.Close()
		return nil, storagedriver.PathNotFoundError{Path: path}
	}
	if res.StatusCode == http.StatusRequestedRangeNotSatisfiable {
		res.Body.Close()
		obj, err := storage.StatObject(d.context(context), d.bucket, name)
		if err != nil {
			return nil, err
		}
		if offset == int64(obj.Size) {
			return ioutil.NopCloser(bytes.NewReader([]byte{})), nil
		}
		return nil, storagedriver.InvalidOffsetError{Path: path, Offset: offset}
	}
	if res.StatusCode < 200 || res.StatusCode > 299 {
		res.Body.Close()
		return nil, fmt.Errorf("storage: can't read object %v/%v, status code: %v", d.bucket, name, res.Status)
	}
	return res.Body, nil
}

// WriteStream stores the contents of the provided io.ReadCloser at a
// location designated by the given path.
// May be used to resume writing a stream by providing a nonzero offset.
// The offset must be no larger than the CurrentSize for this path.
func (d *driver) WriteStream(context ctx.Context, path string, offset int64, reader io.Reader) (totalRead int64, err error) {
	if offset < 0 {
		return 0, storagedriver.InvalidOffsetError{Path: path, Offset: offset}
	}

	s := d.sessions[path]
	if s == nil {
		s = newUploadSession(d.pathToKey(path))
		d.sessions[path] = s
	}

	overlap := s.offset + int64(len(s.buffer)) - offset
	if overlap > 0 {
		totalRead, err = io.Copy(ioutil.Discard, io.LimitReader(reader, overlap))
		if err != nil {
			return totalRead, err
		}
	}

	chunk := make([]byte, maxChunkSize)
	more := true
	for more {
		copy(chunk, s.buffer)
		start := len(s.buffer)
		// add zeros if offset is larger than the current size of the file
		if offset > s.offset+int64(start) {
			count := len(chunk)
			diff := offset - s.offset - int64(start)
			if diff < int64(count) {
				count = int(diff)
			}
			for i := 0; i < count; i++ {
				chunk[start] = 0
				start++
			}
		}

		n, err := reader.Read(chunk[start:])
		if err == io.EOF {
			more = false
		} else if err != nil {
			return totalRead, err
		}
		remainder := (start + n) % minChunkSize
		chunkSize := start + n - remainder
		if chunkSize > 0 {
			if s.sessionURI == "" {
				s.sessionURI, err = startSession(d.client, d.bucket, s.name)
				if err != nil {
					return totalRead, err
				}
			}
			nn, err := putChunk(d.client, s.sessionURI, chunk[0:chunkSize], s.offset, -1)
			s.offset = s.offset + nn

			if err != nil {
				return totalRead, err
			}
		}
		s.buffer = s.buffer[:remainder]
		copy(s.buffer, chunk[chunkSize:chunkSize+remainder])
		totalRead = totalRead + int64(n)
	}
	return totalRead, nil
}

type request func() error

func retry(maxTries int, req request) error {
	backoff := time.Second
	var err error
	for i := 0; i < maxTries; i++ {
		err := req()
		if err == nil {
			return nil
		}

		status := err.(*googleapi.Error)
		if status == nil || (status.Code != 429 && status.Code < http.StatusInternalServerError) {
			return err
		}

		time.Sleep(backoff - time.Second + (time.Duration(rand.Int31n(1000)) * time.Millisecond))
		if i <= 4 {
			backoff = backoff * 2
		}
	}
	return err
}

// CloseStream signals the driver that the last chunk of data has been written
// by WriteStream.
func (d *driver) CloseStream(context ctx.Context, path string) error {
	s, ok := d.sessions[path]
	if !ok {
		return storagedriver.PathNotFoundError{Path: path}
	}
	delete(d.sessions, path)
	// no session started yet just perform a simple upload
	if s.sessionURI == "" {
		return d.PutContent(context, path, s.buffer)
	}
	_, err := putChunk(d.client, s.sessionURI, s.buffer, s.offset, s.length())
	return err
}

// Stat retrieves the FileInfo for the given path, including the current
// size in bytes and the creation time.
func (d *driver) Stat(context ctx.Context, path string) (storagedriver.FileInfo, error) {
	// return stats for uploads that are in progress, as the testsuite assume that
	// Stat works on incomplete uploads
	upload := d.sessions[path]
	if upload != nil {
		fi := storagedriver.FileInfoFields{
			Path:  path,
			Size:  upload.offset + int64(len(upload.buffer)),
			IsDir: false,
		}
		return storagedriver.FileInfoInternal{FileInfoFields: fi}, nil
	}

	var fi storagedriver.FileInfoFields
	//try to get as file
	gcsContext := d.context(context)
	obj, err := storage.StatObject(gcsContext, d.bucket, d.pathToKey(path))
	if err == nil {
		fi = storagedriver.FileInfoFields{
			Path:    path,
			Size:    obj.Size,
			ModTime: obj.Updated,
			IsDir:   false,
		}
		return storagedriver.FileInfoInternal{FileInfoFields: fi}, nil
	}
	//try to get as folder
	dirpath := d.pathToDirKey(path)

	var query *storage.Query
	query = &storage.Query{}
	query.Prefix = dirpath
	query.MaxResults = 1

	objects, err := storage.ListObjects(gcsContext, d.bucket, query)
	if err != nil {
		return nil, err
	}
	if len(objects.Results) < 1 {
		return nil, storagedriver.PathNotFoundError{Path: path}
	}
	fi = storagedriver.FileInfoFields{
		Path:  path,
		IsDir: true,
	}
	obj = objects.Results[0]
	if obj.Name == dirpath {
		fi.Size = obj.Size
		fi.ModTime = obj.Updated
	}
	return storagedriver.FileInfoInternal{FileInfoFields: fi}, nil
}

// List returns a list of the objects that are direct descendants of the
//given path.
func (d *driver) List(context ctx.Context, path string) ([]string, error) {
	var query *storage.Query
	query = &storage.Query{}
	query.Delimiter = "/"
	query.Prefix = d.pathToDirKey(path)
	list := make([]string, 0, 64)
	for {
		objects, err := storage.ListObjects(d.context(context), d.bucket, query)
		if err != nil {
			return nil, err
		}
		for _, object := range objects.Results {
			// GCS does not guarantee strong consistency between
			// DELETE and LIST operationsCheck that the object is not deleted,
			// so filter out any objects with a non-zero time-deleted
			if object.Deleted.IsZero() {
				name := object.Name
				// Ignore objects with names that end with '#' (these are uploaded parts)
				if name[len(name)-1] != '#' {
					name = d.keyToPath(name)
					list = append(list, name)
				}
			}
		}
		for _, subpath := range objects.Prefixes {
			subpath = d.keyToPath(subpath)
			list = append(list, subpath)
		}
		query = objects.Next
		if query == nil {
			break
		}
	}
	return list, nil
}

// Move moves an object stored at sourcePath to destPath, removing the
// original object.
func (d *driver) Move(context ctx.Context, sourcePath string, destPath string) error {
	prefix := d.pathToDirKey(sourcePath)
	gcsContext := d.context(context)
	keys, err := d.listAll(gcsContext, prefix)
	if err != nil {
		return err
	}
	if len(keys) > 0 {
		destPrefix := d.pathToDirKey(destPath)
		copies := make([]string, 0, len(keys))
		sort.Strings(keys)
		var err error
		for _, key := range keys {
			dest := destPrefix + key[len(prefix):]
			_, err = storage.CopyObject(gcsContext, d.bucket, key, d.bucket, dest, nil)
			if err == nil {
				copies = append(copies, dest)
			} else {
				break
			}
		}
		// if an error occurred, attempt to cleanup the copies made
		if err != nil {
			for i := len(copies) - 1; i >= 0; i-- {
				_ = storage.DeleteObject(gcsContext, d.bucket, copies[i])
			}
			return err
		}
		// delete originals
		for i := len(keys) - 1; i >= 0; i-- {
			err2 := storage.DeleteObject(gcsContext, d.bucket, keys[i])
			if err2 != nil {
				err = err2
			}
		}
		return err
	}
	_, err = storage.CopyObject(gcsContext, d.bucket, d.pathToKey(sourcePath), d.bucket, d.pathToKey(destPath), nil)
	if err != nil {
		if status := err.(*googleapi.Error); status != nil {
			if status.Code == http.StatusNotFound {
				return storagedriver.PathNotFoundError{Path: sourcePath}
			}
		}
		return err
	}
	return storage.DeleteObject(gcsContext, d.bucket, d.pathToKey(sourcePath))
}

// listAll recursively lists all names of objects stored at "prefix" and its subpaths.
func (d *driver) listAll(context context.Context, prefix string) ([]string, error) {
	list := make([]string, 0, 64)
	query := &storage.Query{}
	query.Prefix = prefix
	query.Versions = false
	for {
		objects, err := storage.ListObjects(d.context(context), d.bucket, query)
		if err != nil {
			return nil, err
		}
		for _, obj := range objects.Results {
			// GCS does not guarantee strong consistency between
			// DELETE and LIST operationsCheck that the object is not deleted,
			// so filter out any objects with a non-zero time-deleted
			if obj.Deleted.IsZero() {
				list = append(list, obj.Name)
			}
		}
		query = objects.Next
		if query == nil {
			break
		}
	}
	return list, nil
}

// Delete recursively deletes all objects stored at "path" and its subpaths.
func (d *driver) Delete(context ctx.Context, path string) error {
	prefix := d.pathToDirKey(path)
	gcsContext := d.context(context)
	keys, err := d.listAll(gcsContext, prefix)
	if err != nil {
		return err
	}
	if len(keys) > 0 {
		sort.Sort(sort.Reverse(sort.StringSlice(keys)))
		for _, key := range keys {
			if err := storage.DeleteObject(gcsContext, d.bucket, key); err != nil {
				return err
			}
		}
		return nil
	}
	err = storage.DeleteObject(gcsContext, d.bucket, d.pathToKey(path))
	if err != nil {
		if status := err.(*googleapi.Error); status != nil {
			if status.Code == http.StatusNotFound {
				return storagedriver.PathNotFoundError{Path: path}
			}
		}
	}
	return err
}

// URLFor returns a URL which may be used to retrieve the content stored at
// the given path, possibly using the given options.
// Returns ErrUnsupportedMethod if this driver has no privateKey
func (d *driver) URLFor(context ctx.Context, path string, options map[string]interface{}) (string, error) {
	if d.privateKey == nil {
		return "", storagedriver.ErrUnsupportedMethod{}
	}

	name := d.pathToKey(path)
	methodString := "GET"
	method, ok := options["method"]
	if ok {
		methodString, ok = method.(string)
		if !ok || (methodString != "GET" && methodString != "HEAD") {
			return "", storagedriver.ErrUnsupportedMethod{}
		}
	}

	expiresTime := time.Now().Add(20 * time.Minute)
	expires, ok := options["expiry"]
	if ok {
		et, ok := expires.(time.Time)
		if ok {
			expiresTime = et
		}
	}

	opts := &storage.SignedURLOptions{
		GoogleAccessID: d.email,
		PrivateKey:     d.privateKey,
		Method:         methodString,
		Expires:        expiresTime,
	}
	return storage.SignedURL(d.bucket, name, opts)
}

func newUploadSession(name string) *uploadSession {
	return &uploadSession{
		name:   name,
		buffer: make([]byte, 0, minChunkSize-1),
		offset: 0,
	}
}

func (s *uploadSession) length() int64 {
	return s.offset + int64(len(s.buffer))
}

func startSession(client *http.Client, bucket string, name string) (uri string, err error) {
	u := &url.URL{
		Scheme:   "https",
		Host:     "www.googleapis.com",
		Path:     fmt.Sprintf("/upload/storage/v1/b/%v/o", bucket),
		RawQuery: fmt.Sprintf("uploadType=resumable&name=%v", name),
	}
	err = retry(5, func() error {
		req, err := http.NewRequest("POST", u.String(), nil)
		if err != nil {
			return err
		}
		req.Header.Set("X-Upload-Content-Type", "application/octet-stream")
		req.Header.Set("Content-Length", "0")
		resp, err := client.Do(req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		err = googleapi.CheckMediaResponse(resp)
		if err != nil {
			return err
		}
		uri = resp.Header.Get("Location")
		return nil
	})
	return uri, err
}

func putChunk(client *http.Client, sessionURI string, chunk []byte, from int64, totalSize int64) (int64, error) {
	bytesPut := int64(0)
	err := retry(5, func() error {
		req, err := http.NewRequest("PUT", sessionURI, bytes.NewReader(chunk))
		if err != nil {
			return err
		}
		length := int64(len(chunk))
		to := from + length - 1
		size := "*"
		if totalSize >= 0 {
			size = fmt.Sprintf("%v", totalSize)
		}
		req.Header.Set("Content-Type", "application/octet-stream")
		if from == to+1 {
			req.Header.Set("Content-Range", fmt.Sprintf("bytes */%v", size))
		} else {
			req.Header.Set("Content-Range", fmt.Sprintf("bytes %v-%v/%v", from, to, size))
		}
		req.Header.Set("Content-Length", fmt.Sprintf("%v", length))

		resp, err := client.Do(req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		if totalSize < 0 && resp.StatusCode == 308 {
			groups := rangeHeader.FindStringSubmatch(resp.Header.Get("Range"))
			end, err := strconv.ParseInt(groups[2], 10, 64)
			if err != nil {
				return err
			}
			bytesPut = end - from + 1
			return nil
		}
		err = googleapi.CheckMediaResponse(resp)
		if err != nil {
			return err
		}
		bytesPut = to - from + 1
		return nil
	})
	return bytesPut, err
}

func (d *driver) context(context ctx.Context) context.Context {
	return cloud.WithContext(context, dummyProjectID, d.client)
}

func (d *driver) pathToKey(path string) string {
	return strings.TrimRight(d.rootDirectory+strings.TrimLeft(path, "/"), "/")
}

func (d *driver) pathToDirKey(path string) string {
	return d.pathToKey(path) + "/"
}

func (d *driver) keyToPath(key string) string {
	return "/" + strings.Trim(strings.TrimPrefix(key, d.rootDirectory), "/")
}
