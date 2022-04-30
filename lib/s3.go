package lib

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path"
	"strings"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	// "github.com/totoval/framework/helpers/hash"
	"github.com/klauspost/reedsolomon"

	"golang.org/x/net/webdav"
)

const KEEP_FILE_NAME = ".keep"
const KEEP_FILE_CONTENT_TYPE = "application/folder-keep"

// for fileInfo interface

type miniofileInfo struct {
	minio.ObjectInfo
}

func (minfo *miniofileInfo) Name() string {
	log.Println("***************")
	log.Println("Processing name")
	name := minfo.ObjectInfo.Key
	log.Println("name = " + name)
	name = strings.Trim(name, "/")
	log.Println("Trimmed to: " + name)

	if strings.Contains(name, "/") {
		log.Println("string contains /")
	}
	if strings.Contains(name, "/") {
		log.Println("Dir name: " + path.Dir(name))
		log.Println("Replace name: " + name + " to: " + path.Base(name))
		// name = path.Clean(strings.Replace(name, path.Dir(name), "", 1))
		name = path.Base(name)
		log.Println("Cleaned to: " + name)
	}
	log.Println("Key:", minfo.ObjectInfo.Key, "ObjectName:", name)
	log.Println("######end######")
	return name
} // base name of the file !! TODO temply including dir name!
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

// S3 TODO: fake file info, replace with all os.FileInfo and WebDavFile

func NewFS(conf map[string]S3conf) webdav.FileSystem {
	return S3New(conf)
}

type S3conf struct {
	Endpoint        string // endpoint := "play.min.io"
	AccessKeyID     string // accessKeyID := "Q3AM3UQ867SPQQA43P2F"
	SecretAccessKey string // secretAccessKey := "zuf+tfteSlswRu7BJ86wekitnifILbZam1KYY3TG"
	UseSSL          bool   // useSSL := true
	Location        string
	Client          *minio.Client
	Bucket          string
}
type S3confFS struct {
	servers       map[string]S3conf
	rootInfo      *miniofileInfo
	rootFile      *file
	uploadTmpPath string
}

func S3New(conf map[string]S3conf) *S3confFS {
	m := &S3confFS{
		servers:       conf,
		uploadTmpPath: ".",
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
	for index, server := range m.servers {
		if server.Client, err = minio.New(server.Endpoint, &minio.Options{Creds: credentials.NewStaticV4(server.AccessKeyID, server.SecretAccessKey, ""), Secure: server.UseSSL, Region: server.Location}); err != nil {
			log.Println("server index", index, "Error:", err)
			panic(err)
		}
	}

	err = m.MkBucket()
	if err != nil {
		panic(err)
	}

	return m
}

func clearName(name string) (string, error) {
	// 在某些文件系统中，可能有整理路径名的需求，这个函数可能是用于此种用途
	log.Println("enter clearname, name = " + name)
	if name == "/" || name == "" {
		return "", nil
	}
	slashed := strings.HasSuffix(name, "/")
	name = path.Clean(name)
	log.Println("Cleaned name = " + name)
	if !strings.HasSuffix(name, "/") && slashed {
		name += "/"
	}
	// !! TODO Remember to uncommit following code
	//if !strings.HasPrefix(name, "/") {
	//	return "", os.ErrInvalid
	//}
	log.Println("Clean name success, return name = " + name)
	return name, nil
}

func (m *S3confFS) MkBucket() (err error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for index, server := range m.servers {
		exists, err := server.Client.BucketExists(ctx, server.Bucket)
		if err != nil {
			log.Println(err, "op: mkbucket check")
			return err
		}

		if exists {
			log.Println("We already own bucket:", server.Bucket)
			return nil
		}

		// not exist
		if err := server.Client.MakeBucket(ctx, server.Bucket, minio.MakeBucketOptions{Region: server.Location, ObjectLocking: false}); err != nil {
			log.Println(err, "op: mkbucket make")
			return err
		}

		log.Println("server index", index, "Successfully created bucket:", server.Bucket)
	}
	return nil
}

func (m *S3confFS) Mkdir(ctx context.Context, name string, perm os.FileMode) error {
	log.Println("Mkdir:", name)

	name, err := clearName(name)
	if err != nil {
		return err
	}

	fileBytes := bytes.NewBuffer([]byte{})

	for index, server := range m.servers {
		uploadInfo, err := server.Client.PutObject(ctx, server.Bucket, strings.TrimPrefix(path.Join(name, KEEP_FILE_NAME), "/"), bytes.NewBuffer([]byte{}), int64(fileBytes.Len()), minio.PutObjectOptions{ContentType: KEEP_FILE_CONTENT_TYPE})
		if err != nil {
			log.Println(err, "op: mkdir, index: ", index, " name:", path.Join(name, KEEP_FILE_NAME))
			return err
		}
		log.Println("Successfully uploaded bytes: ", uploadInfo, " at index ", index)
		log.Println("mkdir success, name:", name)
	}
	return nil
}

func (m *S3confFS) OpenFile(ctx context.Context, name string, flag int, perm os.FileMode) (webdav.File, error) {
	log.Println("OpenFile", "name:", name, "flag", flag, "perm", perm)

	// what's clearName??
	// test1, _ := clearName("/a/")
	// log.Println("clearName(\"/a/\")", test1)
	// test2, _ := clearName("/aaa")
	// log.Println("clearName(\"/aaa\")", test2)
	// test3, _ := clearName("/aaa/a")
	// log.Println("clearName(\"/aaa/a\")", test3)

	name, err := clearName(name)
	log.Println("openfile after clearName, Name:", name, "err:", err)
	if err != nil {
		return nil, err
	}

	log.Println("minio openfile, Name:", name)
	// TODO: why no proceed here??, m.rootFile
	if len(name) == 0 {
		return m.rootFile, nil
	}

	// file

	// 从存储节点拿文件返回给用户，parityshards
	// do decoding
	parityShards := 4
	readers := make([]io.Reader, parityShards)
	dataShards := 3
	opt := reedsolomon.Option(nil)
	r, err := reedsolomon.NewStream(parityShards, dataShards, opt)
	// out := make([]io.Writer, dataShards)

	for index, server := range m.servers {
		object, err := server.Client.GetObject(ctx, server.Bucket, strings.TrimPrefix(name, "/"), minio.GetObjectOptions{})
		if err != nil {
			log.Println("open name: ", name, "at index: ", index, "error")
		}
		log.Println("open name: ", name, "success")
		readers = append(readers, io.Reader(object))
	}

	// for i := range out {
	// 	out[i] = ioutil.Discard
	// }

	// Encode from input to output.

	b := make([]*bytes.Buffer, dataShards)
	for i := range b {
		b[i] = &bytes.Buffer{}
	}

	out := make([]io.Writer, len(b))
	for i := range b {
		out[i] = b[i]
	}

	err = r.Encode(readers, out)

	if err != nil {
		log.Fatal(err)
	}

	tmpfile, err := ioutil.TempFile("", "tempfile")
	if err != nil {
		log.Fatal(err)
	}

	defer os.Remove(tmpfile.Name()) // clean up

	readout := make([]io.Reader, len(b))
	for i := range b {
		readout[i] = b[i]
		tmpfile.ReadFrom(readout[i])
	}

	// if _, err := tmpfile.Write(content); err != nil {
	// 	tmpfile.Close()
	// 	log.Fatal(err)
	// }

	var obj *minio.Object
	for index, server := range m.servers {
		n, err := server.Client.FPutObject(ctx, server.Bucket, "tempfile", "tempfile", minio.PutObjectOptions{})
		if err != nil {
			fmt.Println(err)
		}
		fmt.Println("Successfully uploaded bytes: ", n, " index ", index)
		obj, err = server.Client.GetObject(ctx, server.Bucket, "tempfile", minio.GetObjectOptions{})
		break
	}

	// if err := tmpfile.Close(); err != nil {
	// 	log.Fatal(err)
	// }

	return &file{m, obj, name}, nil
}

func (m *S3confFS) RemoveAll(ctx context.Context, name string) error {
	log.Println("RemoveAll", name)

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
		for index, server := range m.servers {
			for object := range server.Client.ListObjects(ctx, server.Bucket, minio.ListObjectsOptions{Prefix: name, Recursive: true}) {
				if object.Err != nil {
					log.Println("index", index, object.Err, "op: removeAll, name:", name)
				}
				objectsCh <- object
			}
		}
	}()
	for index, server := range m.servers {
		for rErr := range server.Client.RemoveObjects(ctx, server.Bucket, objectsCh, minio.RemoveObjectsOptions{GovernanceBypass: true}) {
			log.Println("index", index, "Error detected during deletion: ", rErr)

			if rErr.Err != nil {
				return rErr.Err
			}
			// deleteCacheIsDir(rErr.ObjectName)
		}
		if err := server.Client.RemoveObject(ctx, server.Bucket, name, minio.RemoveObjectOptions{}); err != nil {
			return err
		}
		// deleteCacheIsDir(name)
	}

	return nil
}

func (m *S3confFS) Rename(ctx context.Context, oldName, newName string) error {

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
	err = m.WalkDir(ctx, oldParentName, newParentName, oldName)
	if err != nil {
		return err
	}

	// return nil // for test
	return m.RemoveAll(ctx, oldName)
}

func (m *S3confFS) Stat(ctx context.Context, name string) (os.FileInfo, error) {
	log.Println("Stat", "name:", name)

	name, err := clearName(name)
	log.Println("stat after clearName, Name:", name, "err:", err)
	if err != nil {
		return nil, err
	}

	log.Println("minio stat, Name:", name)
	if len(name) == 0 {
		// root dir
		return m.rootInfo, nil
	}

	log.Println("trying to get minio obj, from Bucket:" + m.Bucket + " name: " + name)
	stat, err := m.Client.StatObject(context.Background(), m.Bucket, name, minio.StatObjectOptions{})
	if err != nil {
		if _err, ok := err.(minio.ErrorResponse); ok {
			if _err.Code == "NoSuchKey" {
				// check if is a dir
				if !m.isDir(name) {
					// not exist
					log.Println("m is not a dir, exit")
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
func (m *S3confFS) WalkDir(ctx context.Context, oldParentName, newParentName, oldName string) error {
	log.Println("walk dir, oldParentName:", oldParentName, "newParentName:", newParentName, "oldName:", oldName)

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
	minioObj, err := m.OpenFile(ctx, oldName, 0, 777)
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
		if err := m.WalkDir(ctx, oldName, newName, path.Join(oldName, child.Name())); err != nil {
			return err
		}
	}
	return nil
}
func (m *S3confFS) isDir(name string) bool {
	// check if obj is a directory

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
		// not dir, not exist, or empty dir
		log.Println("Enter ChildrenCount, double checking hidden file")
		//double check dir, if it contains hidden .mindavkeep file
		_, err := m.Client.StatObject(context.Background(), m.Bucket, path.Join(name, KEEP_FILE_NAME), minio.StatObjectOptions{})
		if err != nil {
			log.Println("obj is not a dir, exit")
			// not dir or not exist
			// return cacheIsDir(name, false)
			return false
		}
		log.Println("Has a hidden file")
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

// func isDirCacheKey(name string) string {
// 	const CACHE_KEY_ISDIR = "mindav_isdir_%s"
// 	return fmt.Sprintf(CACHE_KEY_ISDIR, name)
// }

type file struct {
	m *S3confFS
	*minio.Object
	name string
}

func (mo *file) Stat() (os.FileInfo, error) {
	log.Println("file stat, name:", mo.name)
	return mo.m.Stat(context.Background(), mo.name)
}

func (mo *file) ReadFrom(r io.Reader) (n int64, err error) {
	log.Println("file read from, name:", mo.name)

	// memory mode
	uploadInfo, err := mo.m.Client.PutObject(context.Background(), mo.m.Bucket, strings.TrimPrefix(mo.name, "/"), r, -1, minio.PutObjectOptions{ContentType: "application/octet-stream"})
	if err != nil {
		log.Println(err, "op: ReadFrom, name:", mo.name)
		return 0, err
	}
	log.Println("Successfully uploaded bytes: ", uploadInfo.Size)
	return uploadInfo.Size, nil

	// // file mode
	// tmpFilePath := path.Join(mo.m.uploadTmpPath, hash.Md5(mo.name))
	// f, err := os.Create(tmpFilePath)
	// if err != nil {
	// 	return 0, err
	// }
	// defer f.Close()
	// defer func(p string) {
	// 	err = os.RemoveAll(p)
	// 	if err != nil {
	// 		log.Println(err, "op: upload, name:", mo.name, "tempName,", p)
	// 	}
	// }(tmpFilePath)

	// buf := make([]byte, 1024)
	// for {
	// 	// read a chunk
	// 	n, err := r.Read(buf)
	// 	if err != nil && err != io.EOF {
	// 		return 0, err
	// 	}
	// 	if n == 0 {
	// 		break
	// 	}

	// 	// write a chunk
	// 	if _, err := f.Write(buf[:n]); err != nil {
	// 		return 0, err
	// 	}
	// }
	// uploadInfo, err := mo.m.Client.FPutObject(context.Background(), mo.m.Bucket, strings.TrimPrefix(mo.name, "/"), tmpFilePath, minio.PutObjectOptions{ContentType: "application/octet-stream"})
	// if err != nil {
	// 	log.Println(err, "op: ReadFrom, name:", mo.name)
	// 	return 0, err
	// }
	// log.Println("Successfully uploaded object: ", uploadInfo) // TODO
	// log.Println(hash.Md5(mo.name), "op: upload, name:", mo.name)

	// log.Println("Successfully uploaded bytes: ", uploadInfo.Size)
	// return uploadInfo.Size, nil
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
	for object := range mo.m.Client.ListObjects(context.Background(), mo.m.Bucket, minio.ListObjectsOptions{Prefix: name, Recursive: false}) {
		err = object.Err
		if err != nil {
			log.Println(object.Err)
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
