package lib

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"strings"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/totoval/framework/helpers/hash"

	"golang.org/x/net/webdav"
)

const KEEP_FILE_NAME = ".keep"
const KEEP_FILE_CONTENT_TYPE = "application/folder-keep"

// for fileInfo interface

type miniofileInfo struct {
	minio.ObjectInfo
}

func (minfo *miniofileInfo) Name() string {
	name := minfo.ObjectInfo.Key
	name = strings.Trim(name, "/")
	if strings.Contains(name, "/") {
		name = path.Clean(strings.Replace(name, path.Dir(name), "", 1))
	}
	log.Println("Key:", minfo.ObjectInfo.Key, "ObjectName:", name)
	return name
} // base name of the file
func (minfo *miniofileInfo) Size() int64 {
	return minfo.ObjectInfo.Size
} // length in bytes for regular files; system-dependent for others
func (minfo *miniofileInfo) Mode() os.FileMode {
	return 777
} // file mode bits
func (minfo *miniofileInfo) ModTime() time.Time {
	return minfo.ObjectInfo.LastModified
} // modification time
func (minfo *miniofileInfo) IsDir() bool {
	isDir := minfo.ObjectInfo.ContentType == "inode/directory"
	// log minioFileInfo.ObjectInfo.Key, {"IsDir": isDir}
	return isDir
} // abbreviation for Mode().IsDir()
func (minfo *miniofileInfo) Sys() interface{} {
	return nil
} //any

// S3

type S3conf struct {
	Endpoint        string // endpoint := "play.min.io"
	AccessKeyID     string // accessKeyID := "Q3AM3UQ867SPQQA43P2F"
	SecretAccessKey string // secretAccessKey := "zuf+tfteSlswRu7BJ86wekitnifILbZam1KYY3TG"
	UseSSL          bool   // useSSL := true
	Location        string
	Client          *minio.Client
	Bucket          string
	rootInfo        *miniofileInfo
	rootFile        *file
	uploadTmpPath   string
}

func S3New(endpoint, accessKeyID, secretAccessKey string, useSSL bool, bucketName, location string) *S3conf {
	m := &S3conf{
		Endpoint:        endpoint,
		AccessKeyID:     accessKeyID,
		SecretAccessKey: secretAccessKey,
		UseSSL:          useSSL,
		Bucket:          bucketName,
		Location:        location,
		uploadTmpPath:   ".",
		rootInfo: &miniofileInfo{minio.ObjectInfo{
			Key:          "/",
			Size:         0,
			LastModified: time.Now(),
			ContentType:  "inode/directory",
			ETag:         "",
			StorageClass: "",
		}},
	}
	m.rootFile = &file{m, nil, "/"}

	var err error
	if m.Client, err = minio.New(m.Endpoint, &minio.Options{Creds: credentials.NewStaticV4(m.AccessKeyID, m.SecretAccessKey, ""), Secure: m.UseSSL, Region: m.Location}); err != nil {
		panic(err)
	}

	err = m.MkBucket()
	if err != nil {
		panic(err)
	}

	return m
}
func clearName(name string) (string, error) {
	if name == "/" {
		return "", nil
	}
	slashed := strings.HasSuffix(name, "/")
	name = path.Clean(name)
	if !strings.HasSuffix(name, "/") && slashed {
		name += "/"
	}
	if !strings.HasPrefix(name, "/") {
		return "", os.ErrInvalid
	}
	return name, nil
}
func (m *S3conf) MkBucket() (err error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	exists, err := m.Client.BucketExists(ctx, m.Bucket)
	if err != nil {
		log.Println(err, "op: mkbucket check")
		return err
	}

	if exists {
		log.Println("We already own bucket:", m.Bucket)
		return nil
	}

	// not exist
	if err := m.Client.MakeBucket(ctx, m.Bucket, minio.MakeBucketOptions{Region: m.Location, ObjectLocking: false}); err != nil {
		log.Println(err, "op: mkbucket make")
		return err
	}

	log.Println("Successfully created bucket:", m.Bucket)
	return nil
}
func (m *S3conf) Mkdir(name string, perm os.FileMode) error {
	name, err := clearName(name)
	if err != nil {
		return err
	}

	fileBytes := bytes.NewBuffer([]byte{})
	uploadInfo, err := m.Client.PutObject(context.Background(), m.Bucket, strings.TrimPrefix(path.Join(name, KEEP_FILE_NAME), "/"), bytes.NewBuffer([]byte{}), int64(fileBytes.Len()), minio.PutObjectOptions{ContentType: KEEP_FILE_CONTENT_TYPE})
	if err != nil {
		log.Println(err, "op: mkdir", "name:", path.Join(name, KEEP_FILE_NAME))
		return err
	}
	log.Println("Successfully uploaded bytes: ", uploadInfo)
	log.Println("mkdir success, name:", name)
	return nil
}
func (m *S3conf) OpenFile(name string, flag int, perm os.FileMode) (webdav.File, error) {

	name, err := clearName(name)
	if err != nil {
		return nil, err
	}

	log.Println("minio openfile, Name:", name)

	if len(name) == 0 {
		return m.rootFile, nil
	}

	// file
	object, err := m.Client.GetObject(context.Background(), m.Bucket, strings.TrimPrefix(name, "/"), minio.GetObjectOptions{})
	log.Println("open file, name:", name)
	if err != nil {
		return nil, err
	}

	return &file{m, object, name}, nil
}
func (m *S3conf) RemoveAll(name string) error {

	name, err := clearName(name)
	if err != nil {
		return err
	}

	log.Println("minio removeall, Name:", name)

	objectsCh := make(chan minio.ObjectInfo)
	// Send object names that are needed to be removed to objectsCh
	go func() {
		defer close(objectsCh)
		// List all objects from a bucket-name with a matching prefix.
		for object := range m.Client.ListObjects(context.Background(), m.Bucket, minio.ListObjectsOptions{Prefix: name, Recursive: true}) {
			if object.Err != nil {
				log.Println(object.Err, "op: removeAll, name:", name)
			}
			objectsCh <- object
		}
	}()

	for rErr := range m.Client.RemoveObjects(context.Background(), m.Bucket, objectsCh, minio.RemoveObjectsOptions{GovernanceBypass: true}) {
		log.Println("Error detected during deletion: ", rErr)

		if rErr.Err != nil {
			return rErr.Err
		}

		// deleteCacheIsDir(rErr.ObjectName)
	}

	// deleteCacheIsDir(name)
	return m.Client.RemoveObject(context.Background(), m.Bucket, name, minio.RemoveObjectOptions{})
}
func (m *S3conf) Rename(oldName, newName string) error {

	oldParentName, err := clearName(oldName)
	if err != nil {
		return err
	}
	newParentName, err := clearName(newName)
	if err != nil {
		return err
	}

	log.Println("minio rename, Old:", oldName, "New:", newName, "oldParentName:", oldParentName, "newParentName:", newParentName)

	//newName = strings.Replace(newName, path.Dir(oldName), "", 1)
	err = m.WalkDir(oldParentName, newParentName, oldName)
	if err != nil {
		return err
	}

	// return nil // for test
	return m.RemoveAll(oldName)
}
func (m *S3conf) Stat(name string) (os.FileInfo, error) {

	name, err := clearName(name)
	if err != nil {
		return nil, err
	}

	log.Println("minio stat, Name:", name)
	if len(name) == 0 {
		// root dir
		return m.rootInfo, nil
	}

	stat, err := m.Client.StatObject(context.Background(), m.Bucket, name, minio.StatObjectOptions{})
	if err != nil {
		if _err, ok := err.(minio.ErrorResponse); ok {
			if _err.Code == "NoSuchKey" {
				// check is dir
				if !m.isDir(name) {
					// not exist
					return nil, os.ErrNotExist
				}

				// is dir
				theName, err := clearName(name)
				if err != nil {
					return nil, err
				}
				return &miniofileInfo{minio.ObjectInfo{
					Key:          theName,
					Size:         0,
					LastModified: time.Now(),
					ContentType:  "inode/directory",
					ETag:         "",
					StorageClass: "",
				}}, nil
			}
		}
		log.Println(err)
		return nil, err
	}
	return &miniofileInfo{stat}, nil
}
func (m *S3conf) WalkDir(oldParentName, newParentName, oldName string) error {

	oldNameTrim := strings.Trim(oldName, "/")
	newName := newParentName
	if strings.Contains(oldNameTrim, "/") {
		// has child dirs
		newName = strings.Replace(oldName, oldParentName, newParentName, 1)
	}

	log.Println("walkDir, oldParentName:", oldParentName, "newParentName:", newParentName, "oldName:", oldName, "newName:", newName, "isDir:", m.isDir(oldName))

	if !m.isDir(oldName) {
		src := minio.CopySrcOptions{Bucket: m.Bucket, Object: strings.TrimPrefix(oldName, "/")}
		dst := minio.CopyDestOptions{Bucket: m.Bucket, Object: strings.TrimPrefix(newName, "/")}
		uploadInfo, err := m.Client.CopyObject(context.Background(), dst, src)
		if err != nil {
			log.Println(err, "op: walkDir, old:", oldName, "new:", newName)
			return err
		}
		log.Println("Successfully copied object:", uploadInfo)

		return nil
	}

	// is dir, then readdir
	minioObj, err := m.OpenFile(oldName, 0, 777)
	if err != nil {
		log.Println(err, "op: OpenFile, old:", oldName, "new:", newName)
		return err
	}
	oldFileDirChildren, err := minioObj.Readdir(-1)
	if err != nil {
		return err
	}
	for _, child := range oldFileDirChildren {
		log.Println("walkDir oldFileDirChildren, op: walkDir", "oldName:", oldName, "child:", child.Name(), "len:", len(oldFileDirChildren))
		if err := m.WalkDir(oldName, newName, path.Join(oldName, child.Name())); err != nil {
			return err
		}
	}
	return nil
}
func (m *S3conf) isDir(name string) bool {
	if !strings.HasSuffix(name, "/") {
		name = name + "/"
	}

	// cache result
	// isDirPtr := getCacheIsDir(name)
	// if isDirPtr != nil {
	// 	return *isDirPtr
	// }

	childrenCount := 0
	for obj := range m.Client.ListObjects(context.Background(), m.Bucket, minio.ListObjectsOptions{Prefix: name, Recursive: true}) {
		if obj.Err != nil {
			log.Println(obj.Err)
			return false
		}
		childrenCount++
	}

	log.Println("isDir, name:", name, "childrenCount:", childrenCount)

	if childrenCount <= 0 {
		// not dir, not exist

		//double check dir, if it contains hidden .mindavkeep file
		_, err := m.Client.StatObject(context.Background(), m.Bucket, path.Join(name, KEEP_FILE_NAME), minio.StatObjectOptions{})
		if err != nil {
			// not dir or not exist
			// return cacheIsDir(name, false)
			return false
		}
		return true

		// empty dir
		// return cacheIsDir(name, true)
		// } else {
		// 	// not empty dir
		// 	// return cacheIsDir(name, true)
	}
	return true
}

// func cacheIsDir(name string, isDir bool) (_isDir bool) {
// 	cache.Forever(isDirCacheKey(name), isDir)
// 	return isDir
// }

// func getCacheIsDir(name string) (isDirPtr *bool) {
// 	if cache.Has(isDirCacheKey(name)) {
// 		isDir := cache.Get(isDirCacheKey(name)).(bool)
// 		return &isDir
// 	}
// 	return nil
// }

// func deleteCacheIsDir(name string) {
// 	cache.Forget(isDirCacheKey(name))
// }

func isDirCacheKey(name string) string {
	const CACHE_KEY_ISDIR = "mindav_isdir_%s"
	return fmt.Sprintf(CACHE_KEY_ISDIR, name)
}

type file struct {
	m *S3conf
	*minio.Object
	name string
}

func (mo *file) Stat() (os.FileInfo, error) {
	log.Println("file stat, name:", mo.name)
	return mo.m.Stat(mo.name)
}

func (mo *file) ReadFrom(r io.Reader) (n int64, err error) {
	// // memory mode
	// if config.GetBool("webdav.memory_upload_mode") {
	// 	n, err = mo.m.Client.PutObject(ctx, mo.m.bucketName, strings.TrimPrefix(mo.name, "/"), r, -1, minio.PutObjectOptions{ContentType: "application/octet-stream"})
	// 	if err != nil {
	// 		log.Println(err, "op: ReadFrom, name:", mo.name)
	// 		return 0, err
	// 	}
	// 	fmt.Println("Successfully uploaded bytes: ", n)
	// 	return n, nil
	// }

	// file mode
	tmpFilePath := path.Join(mo.m.uploadTmpPath, hash.Md5(mo.name))
	f, err := os.Create(tmpFilePath)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	defer func(p string) {
		err = os.RemoveAll(p)
		if err != nil {
			log.Println(err, "op: upload, name:", mo.name, "tempName,", p)
		}
	}(tmpFilePath)

	buf := make([]byte, 1024)
	for {
		// read a chunk
		n, err := r.Read(buf)
		if err != nil && err != io.EOF {
			return 0, err
		}
		if n == 0 {
			break
		}

		// write a chunk
		if _, err := f.Write(buf[:n]); err != nil {
			return 0, err
		}
	}
	uploadInfo, err := mo.m.Client.FPutObject(context.Background(), mo.m.Bucket, strings.TrimPrefix(mo.name, "/"), tmpFilePath, minio.PutObjectOptions{ContentType: "application/octet-stream"})
	if err != nil {
		log.Println(err, "op: ReadFrom, name:", mo.name)
		return 0, err
	}
	log.Println("Successfully uploaded object: ", uploadInfo) // TODO
	log.Println(hash.Md5(mo.name), "op: upload, name:", mo.name)

	fmt.Println("Successfully uploaded bytes: ", n)
	return n, nil
}

func (mo *file) Write(p []byte) (n int, err error) {
	log.Println(p)
	return len(p), nil // useless
}

func (mo *file) Readdir(count int) (fileInfoList []os.FileInfo, err error) {
	log.Println("file readDir, name:", mo.name)

	name, err := clearName(mo.name)
	if err != nil {
		return nil, err
	}

	if name != "" {
		if !strings.HasSuffix(name, "/") {
			name = name + "/"
		}
	}

	// List all objects from a bucket-name with a matching prefix.
	for object := range mo.m.Client.ListObjects(context.Background(), mo.m.Bucket, minio.ListObjectsOptions{Prefix: name, Recursive: true}) {
		err = object.Err
		if err != nil {
			fmt.Println(object.Err)
			// return
			break
		}

		if object.StorageClass == "" && object.ETag == "" && object.Size == 0 {
			object.ContentType = "inode/directory"
		}

		fileInfoList = append(fileInfoList, &miniofileInfo{object})
	}

	return fileInfoList, err
}
