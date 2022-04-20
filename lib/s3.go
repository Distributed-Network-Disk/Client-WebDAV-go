package lib

import (
	"os"
	"path"
	"strings"

	"github.com/minio/minio-go/v7"
)

type miniofileInfo struct {
	minio.ObjectInfo
}

func (minioFileInfo *miniofileInfo) Name() string {
	name := minioFileInfo.ObjectInfo.Key
	name = strings.Trim(name, "/")

	if strings.Contains(name, "/") {
		name = path.Clean(strings.Replace(name, path.Dir(name), "", 1))
	}
	// log "Key": minioFileInfo.ObjectInfo.Key, "ObjectName": name

	return name
} // base name of the file
func (minioFileInfo *miniofileInfo) Size() int64 {
	return minioFileInfo.ObjectInfo.Size
} // length in bytes for regular files; system-dependent for others
func (minioFileInfo *miniofileInfo) Mode() os.FileMode {
	return 777
} // file mode bits
// func (minioFileInfo *miniofileInfo) ModTime() zone.Time {
// 	return minioFileInfo.ObjectInfo.LastModified
// } // modification time
func (minioFileInfo *miniofileInfo) IsDir() bool {
	isDir := minioFileInfo.ObjectInfo.ContentType == "inode/directory"
	// log minioFileInfo.ObjectInfo.Key, toto.V{"IsDir": isDir}
	return isDir
} // abbreviation for Mode().IsDir()
